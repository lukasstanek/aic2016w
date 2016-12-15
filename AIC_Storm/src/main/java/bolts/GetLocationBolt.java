package bolts;


import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.apache.curator.framework.api.SyncBuilder;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.JedisCommands;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lingfan on 03.11.16.
 */
public class GetLocationBolt extends AbstractRedisBolt {
    private OutputCollector collector;
    private JedisCommands container;
    private final String REDIS_TAG = "LastLocationPropagation";

    public GetLocationBolt(JedisPoolConfig config) {
        super(config);
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, String> map = (HashMap<String, String>) JSON.parse(input);
        container = this.getInstance();
        long lastProgationTime = Long.parseLong(container.get(REDIS_TAG + map.get("id")));

        if(System.currentTimeMillis() - lastProgationTime > 5){

            collector.emit(new Values(input));
            System.out.println(input);
            container.append(REDIS_TAG + map.get("id"), String.valueOf(System.currentTimeMillis()));

        }
        this.returnInstance(container);
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
