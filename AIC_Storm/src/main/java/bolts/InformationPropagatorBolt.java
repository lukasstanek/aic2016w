package bolts;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import util.Util;

import java.util.HashMap;

import static util.Constants.ACTIVE_TAXIS_TAG_DISTANCE_PROPAGATOR_BOLT;
import static util.Constants.LAST_PROPAGATION_TAG_DISTANCE_PROPAGATOR_BOLT;
import static util.Constants.TOTAL_DISTANCE_TAG_DISTANCE_PROPAGATOR_BOLT;

/**
 * Created by thomas on 12.11.16.
 */
public class InformationPropagatorBolt extends AbstractRedisBolt {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(InformationPropagatorBolt.class.getSimpleName());
    long timePassed;

    public InformationPropagatorBolt(JedisPoolConfig config) {
        super(config);
    }

    public void execute(Tuple tuple) {
        JedisCommands jedisCommands = null;

        try {
            jedisCommands = getInstance();
            String distanceString = jedisCommands.get(TOTAL_DISTANCE_TAG_DISTANCE_PROPAGATOR_BOLT);

            long lastPropagation = 0;
            String lastPropagationString = jedisCommands.get(LAST_PROPAGATION_TAG_DISTANCE_PROPAGATOR_BOLT);
            if(lastPropagationString !=  null){
                lastPropagation = Long.parseLong(lastPropagationString);
            }

            if(distanceString == null)
                distanceString = "0";
            double distance = Double.parseDouble(distanceString);

            jedisCommands.set(TOTAL_DISTANCE_TAG_DISTANCE_PROPAGATOR_BOLT, (distance + tuple.getDoubleByField("value"))+"");

            // Store Taxi-Ids with distance
            String activeTaxisSring = jedisCommands.get(ACTIVE_TAXIS_TAG_DISTANCE_PROPAGATOR_BOLT);

            activeTaxisSring = addDistanceToMapString(activeTaxisSring, tuple.getInteger(0), tuple.getDoubleByField("value"));
            jedisCommands.set(ACTIVE_TAXIS_TAG_DISTANCE_PROPAGATOR_BOLT, activeTaxisSring);

            // propagate number of driving taxis and total distance every 5 sec
            if(System.currentTimeMillis() - lastPropagation > 5000){

                int currentNumberOfDrivingTaxis = getNumberOfDrivingTaxis(activeTaxisSring, 0.01);
                // TODO recheck this
                jedisCommands.del(ACTIVE_TAXIS_TAG_DISTANCE_PROPAGATOR_BOLT);

                jedisCommands.set(LAST_PROPAGATION_TAG_DISTANCE_PROPAGATOR_BOLT, String.valueOf(System.currentTimeMillis()));
                collector.emit("TaxiTotal", new Values("Stats", "TaxiTotal", currentNumberOfDrivingTaxis));
                collector.emit("DistanceTotal", new Values("Stats", "DistanceTotal", Util.round(distance + tuple.getDouble(2), 2)));
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
        outputFieldsDeclarer.declareStream("TaxiTotal" ,new Fields("id", "type", "value"));
        outputFieldsDeclarer.declareStream("DistanceTotal" ,new Fields("id", "type", "value"));
    }
}
