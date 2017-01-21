package bolts;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
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
import util.Haversine;
import java.util.HashMap;
import java.util.Map;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;
import static util.Constants.OUT_OF_BOUNDS_NOTIFY_OOB_BOLT;
import static util.Constants.SPEEDING_NOTIFY_SPEEDING_BOLT;


public class NotifySpeedingBolt extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(NotifySpeedingBolt.class.getSimpleName());

    private final double SPEED_LIMIT = 50;

    private JedisCommands container;

    public NotifySpeedingBolt(JedisPoolConfig config) {
        super(config);
    }



    public void execute(Tuple tuple) {
        String taxiId = tuple.getString(0);
        double currentSpeed = tuple.getDouble(2);
        container = this.getInstance();
        String isSpeeding = container.get(SPEEDING_NOTIFY_SPEEDING_BOLT + tuple.getValueByField("id"));



        if(currentSpeed > SPEED_LIMIT){
            if(isSpeeding == null){
                System.out.println("G4T1Bounds: Taxi is speeding #" + taxiId);
                collector.emit(new Values(taxiId, this.getClass().getSimpleName(), "Taxi is speeding! " + currentSpeed + " km/h"));
                container.set(SPEEDING_NOTIFY_SPEEDING_BOLT +taxiId, "1");
            }
        }else{
            if(isSpeeding != null){
                collector.emit(new Values(taxiId, this.getClass().getSimpleName(), "Taxi is not speeding anymore! " + currentSpeed + " km/h"));
                log.info("Taxi not speeding anymore #" + taxiId);
                System.out.println("G4T1Bounds: Taxi back in bounds #" + taxiId);
                container.del(SPEEDING_NOTIFY_SPEEDING_BOLT + taxiId);
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
