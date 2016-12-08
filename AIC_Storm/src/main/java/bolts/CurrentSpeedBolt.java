package bolts;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.storm.shade.org.apache.curator.utils.InternalACLProvider;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.shade.org.jgrapht.experimental.permutation.IntegerPermutationIter;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import util.Haversine;

import java.util.HashMap;


public class CurrentSpeedBolt extends AbstractRedisBolt {
    private static final Logger log = LoggerFactory.getLogger(LocationMonitor.class.getSimpleName());
    private double centerLat = 39.916320;
    private double centerLon = 116.397187;
    private JedisCommands container;
    private final String REDIS_TAG = "LastLocation";

    public CurrentSpeedBolt(JedisPoolConfig config) {
        super(config);
    }


    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        HashMap<String, Object> map = (HashMap<String, Object>) JSON.parse(input);
        container = this.getInstance();

        String lastLocation = container.get(REDIS_TAG + map.get("id"));

        String taxiId = (String) map.get("id");
        Long currentTimestamp = Long.parseLong((String) map.get("timestamp"));
        double currentTaxiLat = Double.parseDouble((String) map.get("latitude"));
        double currentTaxiLon = Double.parseDouble((String) map.get("longitude"));

        if(lastLocation != null){
            String[] data = lastLocation.split(",");
            int lastTimestamp = Integer.parseInt(data[0]);
            double lastTaxtLat = Double.parseDouble(data[1]);
            double lastTaxtLon = Double.parseDouble(data[2]);

            double dist = Haversine.calculate(lastTaxtLat, lastTaxtLon, currentTaxiLat, currentTaxiLon);
            double timeDiff = currentTimestamp - lastTimestamp;


            collector.emit(new Values(taxiId, (dist/timeDiff)*3600));
        }

        container.set(REDIS_TAG+taxiId, map.get("timestamp") + "," + map.get("latitude") + "," + map.get("longitude"));

        this.returnInstance(container);
        collector.ack(tuple);


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "currendSpeed"));

    }
}
