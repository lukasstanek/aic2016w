package com.company;




import bolts.DistanceBolt;
import bolts.DistancePropagator;
import bolts.GetLocationBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

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

        //create our topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", spout);
        //builder.setBolt("getLocation", new GetLocationBolt());


        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);


        builder.setBolt("distanceCalculator", new DistanceBolt())
                .shuffleGrouping("kafkaSpout");
        builder.setBolt("distancePropagator", new DistancePropagator())
                .shuffleGrouping("distanceCalculator");
        StormTopology topology = builder.createTopology();
        cluster.submitTopology("taxilocSample",config,topology);

    }
}
