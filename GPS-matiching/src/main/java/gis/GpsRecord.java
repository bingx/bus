package gis;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GpsRecord {
    public String vehicle;
    public double lng;
    public double lat;
    public Date time;
    public double speed=-1;
    public double direction=-1;
    private static SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
    public GpsRecord(String vehicle,double lng, double lat,Date time,double speed,double direction)
    {
    	this.vehicle=vehicle;
    	this.lng=lng;
    	this.lat=lat;
    	this.time=time;
    	this.speed=speed;
    	this.direction=direction;
    }
    public GpsRecord()
    {
    	
    }
	@Override
	public String toString() {
		return vehicle+","+format.format(time)+","+lng+","+lat+","+speed+","+direction;
	}

}
