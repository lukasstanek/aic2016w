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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lingfan on 03.11.16.
 */
public class GetLocationBolt extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(GetLocationBolt.class.getSimpleName());

    private JedisCommands container;
    private final String REDIS_TAG = "LastLocationPropagation";

    public GetLocationBolt(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, String> map = (HashMap<String, String>) JSON.parse(input);
        container = this.getInstance();
        String lastProgationTime = (String) container.get(REDIS_TAG + map.get("id"));
        if(lastProgationTime == null){
            lastProgationTime = "0";
        }

        if(System.currentTimeMillis()/1000L - Long.parseLong(lastProgationTime) > 5){

            collector.emit(new Values(map.get("id"),
                    this.getClass().getSimpleName(),
                    map.get("latitude") + "," + map.get("longitude")));
            System.out.println(input);
            container.set(REDIS_TAG + map.get("id"), String.valueOf(System.currentTimeMillis()/1000L));

            log.info("Taxi #" + map.get("id") + " at new location lat: " +
                    map.get("latitude") +
                    " lon:" + map.get("longitude"));
        }
        this.returnInstance(container);
        collector.ack(tuple);
    }


    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "type", "value"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
