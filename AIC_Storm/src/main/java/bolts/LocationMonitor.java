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
import util.Haversine;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lukas on 11/12/16.
 */
public class LocationMonitor extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(LocationMonitor.class.getSimpleName());
    private double centerLat = 39.916320;
    private double centerLon = 116.397187;
    private JedisCommands container;
    private final String REDIS_TAG = "OutofBounds";

    public LocationMonitor(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, String> map = (HashMap<String, String>) JSON.parse(input);
        container = this.getInstance();
        String isOutofBounds = container.get(REDIS_TAG + map.get("id"));


        String taxiId = map.get("id");
        double taxiLat = Double.parseDouble(map.get("latitude"));
        double taxiLon = Double.parseDouble(map.get("longitude"));

        double distance = Haversine.calculate(centerLat, centerLon, taxiLat, taxiLon);
        if(distance > 15){
            if(isOutofBounds == null){
                log.info("Taxi out of bounds #" + taxiId);

                collector.emit(new Values("\"id\": \"" + taxiId + "\", \"Warning\":\"Out of Bounds\""));
                container.append(REDIS_TAG +taxiId, "1");
            }
        }else{
            if(isOutofBounds != null){
                log.info("Taxi back in bounds #" + taxiId);
                container.del(REDIS_TAG + taxiId);
            }
        }

        this.returnInstance(container);
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("status"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
