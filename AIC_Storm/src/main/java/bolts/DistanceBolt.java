package bolts;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.JedisCommands;

/**
 * Created by thomas on 12.11.16.
 */
public class DistanceBolt extends AbstractRedisBolt {
    public DistanceBolt(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple input) {
        JedisCommands jedisCommands = null;

        int id = Integer.parseInt(input.getStringByField("id"));
        double longitude = Double.parseDouble(input.getStringByField("longitude"));
        double latitude = Double.parseDouble(input.getStringByField("latitude"));
        int timestamp = input.getIntegerByField("timestamp");

        try {
            jedisCommands = getInstance();
            String oldTimestamp = jedisCommands.get("db-timestamp:"+id);

            if (oldTimestamp != null) {
                double oldLongitude = Double.parseDouble(jedisCommands.get("db-longitude:"+id));
                double oldLatitude = Double.parseDouble(jedisCommands.get("db-latitude:"+id));
                double currentDistance = Double.parseDouble(jedisCommands.get("db-distance:"+id));

                double distance = calculateDistance(oldLongitude, oldLatitude, longitude, latitude);

                jedisCommands.set("db-distance:"+id, (distance+currentDistance)+"");
                this.collector.emit(new Values(id, distance));
            }else{
                jedisCommands.set("db-distance:"+id, "0");
            }
            jedisCommands.set("db-timestamp:"+id, timestamp+"");
            jedisCommands.set("db-longitude:"+id, input.getStringByField("longitude"));
            jedisCommands.set("db-latitude:"+id, input.getStringByField("latitude"));

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(input);
        }
    }

    public  double calculateDistance(double lat1, double lon1, double lat2, double lon2){
        double R = 6371; // km
        double dLat = Math.toRadians(lat2-lat1);
        double dLon = Math.toRadians(lon2-lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c; // km
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "distance"));
    }
}
