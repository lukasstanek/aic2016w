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

import static util.Constants.DISTANCE_TAG_DISTANCE_CALCULATOR_BOLT;
import static util.Constants.LATITUDE_TAG_DISTANCE_CALCULATOR_BOLT;
import static util.Constants.LONGITUDE_TAG_DISTANCE_CALCULATOR_BOLT;

/**
 * Created by thomas on 12.11.16.
 */
public class DistanceBolt extends AbstractRedisBolt {

    public DistanceBolt(JedisPoolConfig config) {
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
            String distanceString = jedisCommands.get(DISTANCE_TAG_DISTANCE_CALCULATOR_BOLT+id);

            double currentDistance = 0, distance = 0;
            if (distanceString != null) {
                double oldLongitude = Double.parseDouble(jedisCommands.get(LONGITUDE_TAG_DISTANCE_CALCULATOR_BOLT+id));
                double oldLatitude = Double.parseDouble(jedisCommands.get(LATITUDE_TAG_DISTANCE_CALCULATOR_BOLT+id));
                currentDistance = Double.parseDouble(distanceString);

                distance = Haversine.calculate(oldLongitude, oldLatitude, longitude, latitude);


            }else{
                jedisCommands.set(DISTANCE_TAG_DISTANCE_CALCULATOR_BOLT+id, "0");
            }

            jedisCommands.set(DISTANCE_TAG_DISTANCE_CALCULATOR_BOLT+id, (distance+currentDistance)+"");
            this.collector.emit(new Values(id, this.getClass().getSimpleName(), distance));

            jedisCommands.set(LONGITUDE_TAG_DISTANCE_CALCULATOR_BOLT+id, longitude+"");
            jedisCommands.set(LATITUDE_TAG_DISTANCE_CALCULATOR_BOLT+id, latitude+"");

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(input);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "type", "value"));
    }
}
