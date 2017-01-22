package bolts;


import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.HashMap;
import java.util.Map;

import static util.Constants.LAST_PROPAGATION_GET_LOCATION_BOLT;

/**
 * Created by lingfan on 03.11.16.
 */
public class LocationBolt extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(LocationBolt.class.getSimpleName());

    private JedisCommands container;

    public LocationBolt(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);

        HashMap<String, String> map = (HashMap<String, String>) JSON.parse(input);
        container = this.getInstance();
        String lastProgationTime = (String) container.get(LAST_PROPAGATION_GET_LOCATION_BOLT + map.get("id"));
        if(lastProgationTime == null){
            lastProgationTime = "0";
        }

        if(System.currentTimeMillis()/1000L - Long.parseLong(lastProgationTime) > 1){

            collector.emit(new Values(map.get("id"),
                    this.getClass().getSimpleName(),
                    map.get("latitude") + "," + map.get("longitude")));
            System.out.println(input);
            container.set(LAST_PROPAGATION_GET_LOCATION_BOLT + map.get("id"), String.valueOf(System.currentTimeMillis()/1000L));

            System.out.println("G4T1Location: Taxi #" + map.get("id") + " at new location lat: " +
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
