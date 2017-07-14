package gis;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RoadMatchResult {
	//原始坐标
	public double src_lng;
	public double src_lat;
	//是否匹配成功及失败原因
	public String reason = "";
	public boolean isMatched = false;
	//匹配上的道路
	public double match_lng;
	public double match_lat;
	public Road road;
	public Date match_time;
	public Date device_time;
	//距离道路入口和出口的距离
	public double length_to_road_entrance=-1;
	public double length_to_road_exit=-1;

	public long stoptime;
	public int status;
	public double speed;
	public double direction;
	public static final int STATUS_MOVING = 1;
	public static final int STATUS_OFF = 0;


	private static SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
	
	public RoadMatchResult(double src_lng,double src_lat,double match_lng,double match_lat,
						   Road road,double length_to_exit,double length_to_entrance,long stoptime,
						   double speed,double direction,
						   Date match_time)
	{
		this.isMatched = true;
		this.src_lng = src_lng;
		this.src_lat = src_lat;
		this.match_lng=match_lng;
		this.match_lat=match_lat;
		this.road=road;
		this.match_time=match_time;
		this.length_to_road_exit = length_to_exit;
		this.length_to_road_entrance = length_to_entrance;
		this.stoptime = stoptime;
		this.speed = speed;
		this.direction = direction;

		if((this.speed < 1.0 && this.stoptime > 60) || (this.stoptime > 60*5)){
			this.status = STATUS_OFF;
		}else{
			this.status = STATUS_MOVING;
		}

	}
	
	public RoadMatchResult(double src_lng,double src_lat,String fail_reason){
		this.src_lng = src_lng;
		this.src_lat = src_lat;
		this.isMatched = false;
		this.reason = fail_reason;
	}
	
	public String toString(){
		if(this.isMatched){
			return format.format(new Date())+","+String.valueOf(match_lng)+","+String.valueOf(match_lat)+","+road.roadID+","+String.valueOf(length_to_road_exit)+","+String.valueOf(length_to_road_entrance);
		}else{
			return this.reason;
		}
	}
}
