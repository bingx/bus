package gis;

import java.text.SimpleDateFormat;
import java.util.Date;

public class VehicleInfo {
	public String road_id;
	public String vehicle;
	public String vehicle_type;
	public float lng;
	public float lat;
	public float speed;
	public float direction;
	public float length_to_entrance;
	public float length_to_exit;
	public Date time;
	public float weight;
	private static SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
	public VehicleInfo(String road_id,String vehicle_type, String vehicle,float lng,float lat,float speed,float direction,float length_to_entrance,float length_to_exit,float weight, Date time){
		this.vehicle_type = vehicle_type;
		this.road_id = road_id;
		this.vehicle = vehicle;
		this.lng = lng;
		this.lat = lat;
		this.speed = speed;
		this.direction = direction;
		this.weight = weight;
		this.length_to_entrance = length_to_entrance;
		this.length_to_exit = length_to_exit;
		this.time = time;
	}
	
	public String toString(){
		return road_id+","+vehicle+","+lng+","+lat+","+speed+","+direction+","+weight+","+format.format(time);
	}
}
