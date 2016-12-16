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
 * Created by ling on 16.12.16.
 */
public class NotifySpeedingBolt implements IRichBolt{
    private final double SPEED_LIMIT = 10;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        String taxiId = tuple.getString(0);
        double currentSpeed = tuple.getDouble(1);

        if(currentSpeed > SPEED_LIMIT){
            System.out.println("G4T1Speeding: taxi: " + taxiId + " is driving more than " + SPEED_LIMIT + "km/h");
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
