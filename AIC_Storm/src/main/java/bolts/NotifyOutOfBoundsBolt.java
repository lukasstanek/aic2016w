package bolts;

import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import util.Haversine;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ling on 15.12.16.
 */
public class NotifyOutOfBoundsBolt implements IRichBolt{

    private final double LAT_LIMIT = 0;
    private final double LONG_LIMIT = 0;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, Object> map = (HashMap<String, Object>) JSON.parse(input);
        double currentTaxiLat = Double.parseDouble((String) map.get("latitude"));
        double currentTaxiLong = Double.parseDouble((String) map.get("longitude"));

        double distanceFromCentre = Haversine.calculate(currentTaxiLat, currentTaxiLong, LAT_LIMIT, LONG_LIMIT);
        if(distanceFromCentre > 10){
            System.out.println("G4T1Bounds: taxi: " + map.get("id") + " is is more than 10km away from centre!");
        }

        if(distanceFromCentre > 15){
            System.out.println("G4T1Bounds: taxi: " + map.get("id") + " is more than 15km away from the centre!");
        }

    }


    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

