package bolts;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by lingfan on 03.11.16.
 */
public class GetLocationBolt implements IRichBolt {
    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        collector.emit(new Values(input));
        collector.ack(tuple);
    }


    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("location"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
