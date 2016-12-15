package bolts;

import com.sun.corba.se.spi.protocol.RequestDispatcherDefault;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

/**
 * Created by ling on 08.12.16.
 */
public class AverageSpeedBolt extends AbstractRedisBolt {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AverageSpeedBolt.class.getSimpleName());
    private final String REDIS_TAG = "AVG-";

    public AverageSpeedBolt(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple input){
        JedisCommands jedisCommands = null;

        try{
            jedisCommands = getInstance();
            String taxiId = input.getString(0);
            double currentSpeed = input.getDouble(1);

            String storedValues = jedisCommands.get(REDIS_TAG+taxiId);
            double newAverageSpeed;
            if(storedValues == null){
                newAverageSpeed = currentSpeed;
                jedisCommands.set(REDIS_TAG+taxiId, ""+newAverageSpeed+", 1");
            }else{
                double currentAverageSpeed = Float.parseFloat(storedValues.split(",")[0]);
                double currentCount = Float.parseFloat(storedValues.split(",")[1]);
                newAverageSpeed = (currentAverageSpeed*currentCount + currentSpeed)/(currentCount + 1);
                currentCount++;
                jedisCommands.set(REDIS_TAG+taxiId,""+newAverageSpeed+","+currentCount);
            }

            System.out.println("G4T1: average speed for taxi "+taxiId+": " + newAverageSpeed );

        }finally {
            if(jedisCommands != null){
                returnInstance(jedisCommands);
            }collector.ack(input);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
