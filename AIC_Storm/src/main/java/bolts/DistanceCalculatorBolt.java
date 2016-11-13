package bolts;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.JedisCommands;
import util.Haversine;

import java.util.HashMap;

/**
 * Created by thomas on 12.11.16.
 */
public class DistanceCalculatorBolt extends AbstractRedisBolt {
    private final String REDIS_TAG = "DCB-";

    public DistanceCalculatorBolt(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple input) {
        JedisCommands jedisCommands = null;

        String inputStr = input.getString(0);
        HashMap<String, String> map = (HashMap<String, String>) JSON.parse(inputStr);

        int id = Integer.parseInt(map.get("id"));
        double longitude = Double.parseDouble(map.get("longitude"));
        double latitude = Double.parseDouble(map.get("latitude"));


        try {
            jedisCommands = getInstance();
            String distanceString = jedisCommands.get(REDIS_TAG+"distance:"+id);

            if (distanceString != null) {
                double oldLongitude = Double.parseDouble(jedisCommands.get(REDIS_TAG+"longitude:"+id));
                double oldLatitude = Double.parseDouble(jedisCommands.get(REDIS_TAG+"latitude:"+id));
                double currentDistance = Double.parseDouble(distanceString);

                double distance = Haversine.calculate(oldLongitude, oldLatitude, longitude, latitude);

                jedisCommands.set(REDIS_TAG+"distance:"+id, (distance+currentDistance)+"");
                this.collector.emit(new Values(id, distance));
            }else{
                jedisCommands.set(REDIS_TAG+"distance:"+id, "0");
            }
            jedisCommands.set(REDIS_TAG+"longitude:"+id, longitude+"");
            jedisCommands.set(REDIS_TAG+"latitude:"+id, latitude+"");

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(input);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "distance"));
    }
}
