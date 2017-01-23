package com.company;




import bolts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import util.Util;

import java.util.Properties;
import java.util.UUID;

import static util.Constants.*;

public class Main {
    public static String ZOOKEEPER_HOST = "localhost";
    public static String ZOOKEEPER_PORT = "2181";
    public static String KAFKA_HOST = "localhost";
    public static String KAFKA_PORT = "9092";
    public static String REDIS_HOST = "localhost";
    public static int REDIS_PORT = 6379;


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        if(args.length > 0){
            ZOOKEEPER_HOST = args[0];
        }
        if(args.length > 1){
            ZOOKEEPER_PORT = args[1];
        }
        if(args.length > 2){
            KAFKA_HOST = args[2];
        }
        if(args.length > 3){
            KAFKA_PORT = args[3];
        }
        if(args.length > 4){
            REDIS_HOST = args[4];
        }
        if(args.length > 5){
            REDIS_PORT = Integer.parseInt(args[5]);
        }



        //config
        Config clusterConfig = new Config();
        clusterConfig.setDebug(true);
        clusterConfig.setNumWorkers(1);

        //zookeeper brokerhost
        BrokerHosts brokerHost = new ZkHosts(ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT);

        //kafka config
        SpoutConfig spoutConfig = new SpoutConfig(brokerHost,DATASOURCE,"/taxilocs", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //kafka spout
        KafkaSpout spout = new KafkaSpout(spoutConfig);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();

        //create our topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT, spout);

        builder.setBolt(LOCATION_BOLT, new LocationBolt(jedisPoolConfig))
               .shuffleGrouping(KAFKA_SPOUT);

        builder.setBolt(NOTIFY_OOB_BOLT, new NotifyOutofBoundsBolt(jedisPoolConfig))
                .shuffleGrouping(KAFKA_SPOUT);

        builder.setBolt(DISTANCE_BOLT, new DistanceBolt(jedisPoolConfig))
                .shuffleGrouping(KAFKA_SPOUT);

        builder.setBolt(INFORMATION_PROPAGATOR_BOLT, new InformationPropagatorBolt(jedisPoolConfig))
                .shuffleGrouping(DISTANCE_BOLT);


        builder.setBolt(CURRENT_SPEED_BOLT, new CurrentSpeedBolt(jedisPoolConfig))
                .shuffleGrouping(KAFKA_SPOUT);


        builder.setBolt(AVERAGE_SPEED_BOLT, new AverageSpeedBolt(jedisPoolConfig))
            .shuffleGrouping(CURRENT_SPEED_BOLT);

        builder.setBolt(NOTIFY_SPEEDING_BOLT, new NotifySpeedingBolt(jedisPoolConfig))
                .shuffleGrouping(CURRENT_SPEED_BOLT);

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
        kafkaProps.put("acks", "1");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt kafkaBolt = new KafkaBolt()
                .withProducerProperties(kafkaProps)
                .withTopicSelector(new KafkaTopicSelector() {
                    public String getTopic(Tuple tuple) {
                        return KAFKA_OUTPUT;
                    }
                })
                .withTupleToKafkaMapper(new TupleToKafkaMapper() {
                    public Object getKeyFromTuple(Tuple tuple) {
                        return tuple.getStringByField("type");
                    }

                    public Object getMessageFromTuple(Tuple tuple) {
                        String id = tuple.getStringByField("id");
                        String type = tuple.getStringByField("type");
                        String value = tuple.getValueByField("value").toString();
                        return id+","+type+","+value;
                    }
                });

        builder.setBolt(OUTPUT_BOLT, kafkaBolt)
                .shuffleGrouping(NOTIFY_OOB_BOLT)
                .shuffleGrouping(LOCATION_BOLT)
                .shuffleGrouping(NOTIFY_SPEEDING_BOLT)
                .shuffleGrouping(INFORMATION_PROPAGATOR_BOLT, "TaxiTotal")
                .shuffleGrouping(INFORMATION_PROPAGATOR_BOLT, "DistanceTotal");



        if (args != null && args.length > 0) {
            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY, clusterConfig, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology(TOPOLOGY, clusterConfig, topology);
        }

    }

    public static void testHaversine(String[] args){
        double lat = 39.916320;
        double long1 = 116.397187;

        double lat2 = 39.913785;
        double long2 = 116.397681;

        System.out.println("distance: " + Util.Haversine(lat,long1,lat2,long2));
    }
}
