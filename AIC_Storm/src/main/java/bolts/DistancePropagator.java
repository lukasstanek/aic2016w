package bolts;

import org.apache.log4j.Logger;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

/**
 * Created by thomas on 12.11.16.
 */
public class DistancePropagator extends AbstractRedisBolt {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DistancePropagator.class.getSimpleName());

    public DistancePropagator(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;

        try {
            jedisCommands = getInstance();
            String distanceString = jedisCommands.get("dpb-total-distance");
            if(distanceString == null)
                distanceString = "0";
            double distance = Double.parseDouble(distanceString);

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
