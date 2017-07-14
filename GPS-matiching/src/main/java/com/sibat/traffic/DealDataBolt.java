package com.sibat.traffic;

import gis.GpsRecord;
import gis.Road;
import gis.RoadMatchResult;
import gis.RoadMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static util.WriteFile.writeToFile;


/**
 * Created by User on 2017/5/19.
 */
public class DealDataBolt {
    private Logger logger;

    static String path = "/home/datum/storm/shpfile/guangdong_polyline.shp";
    public static RoadMatcher roadMatcher = null;
    private static SimpleDateFormat format= new SimpleDateFormat("yy-MM-dd HH:mm:ss");

    public DealDataBolt() {
        this.logger = LoggerFactory.getLogger(this.getClass());
        roadMatcher = new RoadMatcher(path);

    }

    public void dealData(Record info ) throws IOException {

      //  System.out.println("matching....");
        String vehicle_type = info.vehicletype;
        String vehicle=info.vehicle;
        double longitude = Double.parseDouble(String.valueOf(info.lng));
        double latitude = Double.parseDouble(String.valueOf(info.lat));
        float speed = info.speed;
        float direction = info.direction;
        Date time=new Date();
        time.setTime(info.timestamp*1000);
        GpsRecord gps_record = new GpsRecord(vehicle,longitude,latitude,time,speed, direction);
        RoadMatchResult match_result = roadMatcher.matchRoad(gps_record);
        if (match_result != null && match_result.isMatched) {

            Road road = match_result.road;
            String roadID = road.roadID;

            if(match_result.status == RoadMatchResult.STATUS_MOVING){
                System.out.println("send   ------------------->:data time =>  Match result<"+format.format(time) +"<    >"+match_result.toString());
                //writeToFile(format.format(time) + "," +roadID+","+vehicle+","+vehicle_type+","+speed,"05-19");
                String saveInfor = format.format(time) + "," +roadID+","+vehicle+","+vehicle_type+","+speed;
                writeToFile(saveInfor);

            }
        }


    }
}
