package bolts;

import org.apache.log4j.Logger;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

/**
 * Created by thomas on 12.11.16.
 */
public class DistancePropagator extends AbstractRedisBolt {

    Logger log;

    public DistancePropagator(JedisPoolConfig config) {
        super(config);
        try{
            getInstance().set("dpb-total-distance", "0");
        }catch(Exception e){}
        log = Logger.getLogger(DistancePropagator.class);
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;

        try {
            double distance = Double.parseDouble(jedisCommands.get("dpb-total-distance"));
            jedisCommands.set("dpb-total-distance", (distance + tuple.getDouble(1))+"");
            log.info("TOTAL DISTANCE: "+(distance + tuple.getDouble(1)));
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
