package bolts;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import util.Util;

import java.util.HashMap;

import static util.Constants.LAST_LOCATION_CURRENT_SPEED_BOLT;


public class CurrentSpeedBolt extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(NotifyOutofBoundsBolt.class.getSimpleName());
    private double centerLat = 39.916320;
    private double centerLon = 116.397187;
    private JedisCommands container;

    public CurrentSpeedBolt(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, Object> map = (HashMap<String, Object>) JSON.parse(input);
        container = this.getInstance();

        String lastLocation = container.get(LAST_LOCATION_CURRENT_SPEED_BOLT + map.get("id"));

        String taxiId = (String) map.get("id");
        Long currentTimestamp = Long.parseLong((String) map.get("timestamp"));
        double currentTaxiLat = Double.parseDouble((String) map.get("latitude"));
        double currentTaxiLon = Double.parseDouble((String) map.get("longitude"));
        double currentSpeed = 0;
        if(lastLocation != null){
            String[] data = lastLocation.split(",");
            int lastTimestamp = Integer.parseInt(data[0]);
            double lastTaxtLat = Double.parseDouble(data[1]);
            double lastTaxtLon = Double.parseDouble(data[2]);

            double dist = Util.Haversine(lastTaxtLat, lastTaxtLon, currentTaxiLat, currentTaxiLon);
            double timeDiff = currentTimestamp - lastTimestamp;

            // in case we have a time diff of 0
            if(timeDiff == 0){
                this.returnInstance(container);
                collector.ack(tuple);
                return;
            }
            currentSpeed = Math.abs((dist/timeDiff)*3600);

            if(Double.isNaN(currentSpeed)){
                log.info("current speed is not a number");
            }

        }

        log.info("Current speed for Taxi #" + taxiId + ": " + currentSpeed + " km/h");
        collector.emit(new Values(taxiId, this.getClass().getSimpleName(), Util.round(currentSpeed, 2)));
        System.out.println("G4T1CurrentSpeed: taxi: " + taxiId + " current speed: "+ currentSpeed);

        container.set(LAST_LOCATION_CURRENT_SPEED_BOLT+taxiId, map.get("timestamp") + "," + map.get("latitude") + "," + map.get("longitude"));

        this.returnInstance(container);
        collector.ack(tuple);


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "type","value"));

    }
}
