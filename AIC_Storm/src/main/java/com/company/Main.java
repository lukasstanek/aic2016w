package com.company;




import bolts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import util.Haversine;

import java.util.UUID;

public class Main {

    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();

        //config
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(1);

        //zookeeper brokerhost
        BrokerHosts host = new ZkHosts("172.17.0.1:2181");
        //kafka config
        SpoutConfig spoutConfig = new SpoutConfig(host,"taxilocs","/taxilocs", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //kafka spout
        KafkaSpout spout = new KafkaSpout(spoutConfig);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder().setHost("172.17.0.1").setPort(6379).build();

        //create our topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", spout);

        builder.setBolt("getLocation", new GetLocationBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("monitorLocation", new LocationMonitor(jedisPoolConfig)).shuffleGrouping("getLocation");

        builder.setBolt("distanceCalculator", new DistanceCalculatorBolt(jedisPoolConfig))
                .shuffleGrouping("kafkaSpout");
        builder.setBolt("distancePropagator", new DistancePropagator(jedisPoolConfig))
                .shuffleGrouping("distanceCalculator");
        builder.setBolt("currentDistance", new CurrentSpeedBolt(jedisPoolConfig))
                .shuffleGrouping("kafkaSpout");

        StormTopology topology = builder.createTopology();
        cluster.submitTopology("taxilocSample",config,topology);
        //cluster.shutdown();
    }

    public static void testHaversine(String[] args){
        double lat = 39.916320;
        double long1 = 116.397187;

        double lat2 = 39.913785;
        double long2 = 116.397681;

        System.out.println("distance: " + Haversine.calculate(lat,long1,lat2,long2));
    }
}
