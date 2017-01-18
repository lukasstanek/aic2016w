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

import static util.Constants.OUT_OF_BOUNDS_15_NOTIFY_OOB_BOLT;
import static util.Constants.OUT_OF_BOUNDS_NOTIFY_OOB_BOLT;

/**
 * Created by lukas on 11/12/16.
 */
public class NotifyOutofBoundsBolt extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(NotifyOutofBoundsBolt.class.getSimpleName());
    private double centerLat = 39.916320;
    private double centerLon = 116.397187;
    private JedisCommands container;

    public NotifyOutofBoundsBolt(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, String> map = (HashMap<String, String>) JSON.parse(input);
        container = this.getInstance();
        String isOutofBounds = container.get(OUT_OF_BOUNDS_NOTIFY_OOB_BOLT + map.get("id"));


        String taxiId = map.get("id");
        double taxiLat = Double.parseDouble(map.get("latitude"));
        double taxiLon = Double.parseDouble(map.get("longitude"));

        double distance = Haversine.calculate(centerLat, centerLon, taxiLat, taxiLon);
        if(distance > 10){
            if(isOutofBounds == null){
                System.out.println("G4T1Bounds: Taxi out of bounds #" + taxiId);

                collector.emit(new Values(taxiId, this.getClass().getSimpleName(), ">10"));
                container.set(OUT_OF_BOUNDS_NOTIFY_OOB_BOLT +taxiId, "1");
            }
        }else{
            if(isOutofBounds != null){
                collector.emit(new Values(taxiId, this.getClass().getSimpleName(), "<10"));
                log.info("Taxi back in bounds #" + taxiId);
                System.out.println("G4T1Bounds: Taxi back in bounds #" + taxiId);
                container.del(OUT_OF_BOUNDS_NOTIFY_OOB_BOLT + taxiId);
            }
        }
        if(distance > 15){
            if(isOutofBounds == null){
                System.out.println("G4T1Bounds: Taxi out of 15km radius #" + taxiId);

                collector.emit(new Values(taxiId, this.getClass().getSimpleName(), ">15"));
                container.set(OUT_OF_BOUNDS_15_NOTIFY_OOB_BOLT +taxiId, "1");
            }
        }else{
            if(isOutofBounds != null){
                collector.emit(new Values(taxiId, this.getClass().getSimpleName(), "<15"));
                log.info("Taxi back in bounds #" + taxiId);
                System.out.println("G4T1Bounds: Taxi back in 15km radius #" + taxiId);
                container.del(OUT_OF_BOUNDS_15_NOTIFY_OOB_BOLT + taxiId);
            }
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
