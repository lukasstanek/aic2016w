package com.company;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Topologies;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.TopologyBuilder;
import bolts.GetLocationBolt;
import storm.kafka.*;

import java.util.UUID;

public class Main {

    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();

        //config
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(1);

        //zookeeper brokerhost
        BrokerHosts host = new ZkHosts("localhost:2181");
        //kafka config
        SpoutConfig spoutConfig = new SpoutConfig(host,"taxilocs","/taxilocs", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //kafka spout
        KafkaSpout spout = new KafkaSpout(spoutConfig);

        //create our topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", spout);
        builder.setBolt("getLocation", new GetLocationBolt());
        StormTopology topology = builder.createTopology();
        cluster.submitTopology("taxilocSample",config,topology);

    }
}
