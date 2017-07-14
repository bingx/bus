package com.sibat.traffic;

/**
 * Created by User on 2017/5/19.
 */

/**
 * to deal with the data read from the text
 */

//new Values(vehicletype, vehicleid, x, y,
               // timestamp, deviceid, speed, direction, extrainfo));
public class Record {
    public String vehicletype;
    public String vehicle;
    public float lng;
    public float lat;
    public long timestamp;
    public String deviceid;
    public float speed;
    public float direction;
    public String extrainfo;

    public Record(String vehicletype, String vehicleid, float lng, float lat, long timestamp, String deviceid, float speed, float direction, String extrainfo) {
        this.vehicletype = vehicletype;
        this.vehicle = vehicleid;
        this.lng = lng;
        this.lat = lat;
        this.timestamp = timestamp;
        this.deviceid = deviceid;
        this.speed = speed;
        this.direction = direction;
        this.extrainfo = extrainfo;
    }
}
