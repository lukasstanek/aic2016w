package bolts;

import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import util.Haversine;
import java.util.HashMap;
import java.util.Map;


public class NotifySpeedingBolt implements IRichBolt{
    private final double SPEED_LIMIT = 40;

    private OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String taxiId = tuple.getString(0);
        double currentSpeed = tuple.getDouble(2);

        if(currentSpeed > SPEED_LIMIT){
            System.out.println("G4T1Speeding: taxi: " + taxiId + " is driving more than " + SPEED_LIMIT + "km/h");
            collector.emit(new Values(taxiId, this.getClass().getSimpleName(), "Taxi is speeding! " + currentSpeed + " km/h"));
        }

    }


    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "type", "value"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
