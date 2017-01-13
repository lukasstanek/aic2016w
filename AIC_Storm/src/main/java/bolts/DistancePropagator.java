package bolts;

import org.apache.log4j.Logger;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by thomas on 12.11.16.
 */
public class DistancePropagator extends AbstractRedisBolt {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DistancePropagator.class.getSimpleName());
    long timePassed, lastPropagation;

    public DistancePropagator(JedisPoolConfig config) {
        super(config);
        lastPropagation = System.currentTimeMillis();
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;

        try {
            jedisCommands = getInstance();
            String distanceString = jedisCommands.get("dpb-total-distance");
            if(distanceString == null)
                distanceString = "0";
            double distance = Double.parseDouble(distanceString);

            jedisCommands.set("dpb-total-distance", (distance + tuple.getDoubleByField("value"))+"");

            // Store Taxi-Ids with distance
            String activeTaxisSring = jedisCommands.get("dpb-active-taxis");
            activeTaxisSring = addDistanceToMapString(activeTaxisSring, tuple.getInteger(0), tuple.getDoubleByField("value"));
            jedisCommands.set("dpb-active-taxis", activeTaxisSring);

            // propagate number of driving taxis and total distance every 5 sec
            if(System.currentTimeMillis() - lastPropagation > 1000){

                int currentNumberOfDrivingTaxis = getNumberOfDrivingTaxis(activeTaxisSring, 0.001);
                // TODO recheck this
                //jedisCommands.del("dpb-active-taxis");
                lastPropagation = System.currentTimeMillis();

                System.out.println("G4T1Distance: Current Taxis driving: " + currentNumberOfDrivingTaxis);
                System.out.println("G4T1Distance: Total Distance: "+(distance + tuple.getDouble(2)));
            }

        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            this.collector.ack(tuple);

        }
    }

    private String addDistanceToMapString(String activeTaxisMapSring, int id, double distance){
        HashMap<String, String> taxis;
        if(activeTaxisMapSring == null)
            taxis = new HashMap<String,String>();
        else
            taxis = (HashMap<String, String>) JSON.parse(activeTaxisMapSring);

        String currDistaneString = taxis.get(id+"");
        double currentDistance;
        if(currDistaneString == null)
            currentDistance = 0;
        else
            currentDistance = Double.parseDouble(currDistaneString);

        currentDistance += distance;
        taxis.put(id+"", currentDistance+"");
        JSONObject taxisJson = new JSONObject();
        taxisJson.putAll(taxis);
        return taxisJson.toJSONString();
    }

    private int getNumberOfDrivingTaxis(String activeTaxisMapSring, double threshold){
        HashMap<String, String> taxis;
        if(activeTaxisMapSring == null)
            return 0;

        int count = 0;
        taxis = (HashMap<String, String>) JSON.parse(activeTaxisMapSring);
        for(String distance : taxis.values()){
            if(Double.parseDouble(distance) > threshold)
                count++;
        }
        return count;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
