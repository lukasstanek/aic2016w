package util;

/**
 * Created by ling on 18.01.17.
 */
public class Constants {
    //bolt names
    public static final String DATASOURCE = "taxilocs";
    public static final String KAFKA_SPOUT = "kafkaSpout";
    public static final String AVERAGE_SPEED_BOLT = "averageSpeedBolt";
    public static final String CURRENT_SPEED_BOLT = "currentSpeedBole";
    public static final String DISTANCE_BOLT = "distanceBolt";
    public static final String INFORMATION_PROPAGATOR_BOLT = "InformationPropagatorBolt";
    public static final String LOCATION_BOLT = "locationBolt";
    public static final String NOTIFY_OOB_BOLT = "notifyOutOfBoundsBolt";
    public static final String NOTIFY_SPEEDING_BOLT = "notifySpeedingBolt";

    // redis tags
    public static final String AVG_SPEED_AVG_SPEED_BOLT = "AVG_";
    public static final String LAST_LOCATION_CURRENT_SPEED_BOLT =  "LAST_LOCATION_";
    public static final String LONGITUDE_TAG_DISTANCE_CALCULATOR_BOLT = "DCB_LONGITUDE_";
    public static final String LATITUDE_TAG_DISTANCE_CALCULATOR_BOLT = "DCB_LATITUDE_";
    public static final String DISTANCE_TAG_DISTANCE_CALCULATOR_BOLT = "DCB_DISTANCE_";
    public static final String LAST_PROPAGATION_TAG_DISTANCE_PROPAGATOR_BOLT = "DPB_LAST_PROPAGATION";
    public static final String TOTAL_DISTANCE_TAG_DISTANCE_PROPAGATOR_BOLT = "DPB_TOTAL_DISTANCE";
    public static final String ACTIVE_TAXIS_TAG_DISTANCE_PROPAGATOR_BOLT = "DPB_ACTIVE_TAXIS";
    public static final String LAST_PROPAGATION_GET_LOCATION_BOLT = "LAST_PROPAGATION_";

}
