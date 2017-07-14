package gis;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.util.*;

public class RoadMatcher {

	private HashMap<String, ArrayList<Road>> roadmap = new HashMap<String, ArrayList<Road>>();
	private HashMap<String,List<RoadMatchResult>> history=new HashMap<String,List<RoadMatchResult>>();
	private static Logger logger;


	public RoadMatcher(String path) {
		try {
			this.roadmap = this.read(path);

			logger = LoggerFactory.getLogger(RoadMatcher.class);

		}catch(Exception ex){
			ex.printStackTrace();
		}
	}


	private HashMap<String, ArrayList<Road>> read(String path) throws IOException {

		//载入SHP文件
		System.out.println("shp path :  "+path);
		File file = new File(path);
		ShapefileDataStore shpDataStore = null;
		try {
			shpDataStore = new ShapefileDataStore(file.toURL());
			shpDataStore.setStringCharset(Charset.forName("GBK"));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		// Feature Access
		String typeName = shpDataStore.getTypeNames()[0];
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = null;
		try {
			featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) shpDataStore
					.getFeatureSource(typeName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		FeatureCollection<SimpleFeatureType, SimpleFeature> result = null;
		try {
			result = featureSource.getFeatures();
		} catch (IOException e) {
			e.printStackTrace();
		}

		FeatureIterator<SimpleFeature> itertor = result.features();
		try {
			while (itertor.hasNext()) {
				// Data Reader
				SimpleFeature feature = itertor.next();
				MultiLineString geom =(MultiLineString)feature.getDefaultGeometry();

				double width = Double.parseDouble(feature.getAttribute("Width").toString());
				double length = Double.parseDouble(feature.getAttribute("Length").toString());
				double negative_max_speed = Double.parseDouble(feature.getAttribute("NegMaxSped").toString());
				double positive_max_speed = Double.parseDouble(feature.getAttribute("PosMaxSped").toString());

				String roadID = feature.getAttribute("ID").toString();
				String roadName = feature.getAttribute("Name").toString();

				LineString geometry=(LineString)geom.getGeometryN(0);

				Road road=new Road(roadID,roadName, width,negative_max_speed,positive_max_speed,geometry);
				HashSet<Integer> lngs = new HashSet<Integer>();
				HashSet<Integer> lats = new HashSet<Integer>();
				for(int i=0;i<geometry.getNumPoints();i++){
					Point p = geometry.getPointN(i);
					lngs.add(new Integer((int)(p.getX()*100)));
					lats.add(new Integer((int)(p.getY()*100)));
				}

				String mapID;
				for(Integer x: lngs){
					for(Integer y:lats){
						mapID = x + "_" + y;
						if (!roadmap.containsKey(mapID)) {
							ArrayList<Road> roadList = new ArrayList<Road>();
							roadList.add(road);
							roadmap.put(mapID, roadList);
						} else {
							ArrayList<Road> roadList = roadmap.get(mapID);
							roadList.add(road);           //  加入roadmap.put(mapID, roadList);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			itertor.close();
			shpDataStore.dispose();
		}
		return roadmap;
	}


	public RoadMatchResult matchRoad(GpsRecord record_to_match)
	{
		//long start = new Date().getTime();

		double src_lng = record_to_match.lng;
		double src_lat = record_to_match.lat;

		RoadMatchResult result=null;
		Point matched_point = null;
		Date time=record_to_match.time;
		String device = record_to_match.vehicle;
		ArrayList<Road> road_list = new ArrayList<Road>();

		Point point = GeometryHelper.CreatePoint(record_to_match.lng,record_to_match.lat);


		//异常点检测
		if((record_to_match.lng < 73.33 || record_to_match.lng > 135.05) ||
				(record_to_match.lat < 3.51 || record_to_match.lat > 53.33)){
			//匹配点不在中国境内
			result = new RoadMatchResult(src_lng,src_lat,"OUTSIDE_CHINA");
		}else{

			//clear history matchedRecord for this device by time;
			if(this.history.containsKey(device))
			{
				List<RoadMatchResult> matched_list=this.history.get(device);

				// Remove the elements from the end
				for(int i=matched_list.size()-1;i>=0;i--)
				{
					if(matched_list.get(i).match_time.getTime()-time.getTime()>1000*60*5){
						matched_list.remove(i);
					}
				}
			}

			// Load 9 grids from map
			int x=(int)(record_to_match.lng*100);
			int y=(int)(record_to_match.lat*100);
			String mapID="";
			for(int i=-1;i<2;i++)
			{
				for(int j=-1;j<2;j++)
				{
					mapID=(x+i)+"_"+(y+j);
					if(this.roadmap.containsKey(mapID))
					{
						road_list.addAll(roadmap.get(mapID));
					}
				}
			}

			if(road_list.size()==0){
				result = new RoadMatchResult(src_lng,src_lat,"EMPTY_ROADS");
			}
			else{
				if(this.history.containsKey(device)&&this.history.get(device).size()>=1){
					result = multiPointMatch(record_to_match, road_list, history.get(device));
				}
				else{
					result=singlePointMatch(record_to_match,road_list);
				}
			}
		}


		if(result.isMatched){
			matched_point = GeometryHelper.CreatePoint(result.match_lng, result.match_lat);
			double distance = GeometryHelper.GetDistanceInKm(point, matched_point);
			if(distance > 0.050){
				result = new RoadMatchResult(src_lng,src_lat,result.toString()+","+String.valueOf(distance) + ",DISTANCE_BEYOND_THRESHOLD");
			}
		}

		if(result.isMatched){
			if(history.containsKey(device)){
				history.get(device).add(result);
			}else{
				ArrayList<RoadMatchResult> buffer = new ArrayList<RoadMatchResult>();
				buffer.add(result);
				history.put(device, buffer);
			}
		}else{

		}

		return result;
	}


	/**
	 * 单点距离匹配
	 * @param record_to_match 要匹配的记录
	 * @param road_list 道路列表
	 * @return
	 */
	public RoadMatchResult singlePointMatch(GpsRecord record_to_match,ArrayList<Road> road_list)
	{
		double lng = record_to_match.lng;
		double lat = record_to_match.lat;
		Date time = record_to_match.time;

		Road match_road=null;

		Point point = GeometryHelper.CreatePoint(lng,lat);

		double distance=Double.MAX_VALUE;
		for(Road road : road_list)
		{
			double temp_distance=point.distance(road.geometry);
			if(temp_distance<distance)
			{
				match_road=road;
				distance=temp_distance;
			}
		}

		//计算距离道路出口的距离
		Point match_point = match_road.nearestPointTo(point);
		double[] length_to_entrance_and_exit = match_road.lengthToEntranceAndExit(match_point, record_to_match.direction);
		double length_to_entrance = length_to_entrance_and_exit[0];
		double length_to_exit = length_to_entrance_and_exit[1];
		return new RoadMatchResult(lng,lat,
				match_point.getX(),match_point.getY(),
				match_road,length_to_exit,length_to_entrance,
				0L,
				-1,-1,
				time);
	}

	/**
	 * 多点匹配
	 * @param record_to_match 要匹配的记录
	 * @param road_list 道路列表 
	 * @param history 历史记录
	 * @return
	 */
	public RoadMatchResult multiPointMatch(GpsRecord record_to_match,ArrayList<Road> road_list,List<RoadMatchResult> history){
		double lng = record_to_match.lng;
		double lat = record_to_match.lat;
		Date time = record_to_match.time;
		Road match_road=null;
		RoadMatchResult result;

		Point point = GeometryHelper.CreatePoint(lng,lat);
		int index_last=history.size()-1;
		RoadMatchResult lastRecord=history.get(index_last);

		Point last_point=GeometryHelper.CreatePoint(lastRecord.match_lng,lastRecord.match_lat);
		Point last_src_point = GeometryHelper.CreatePoint(lastRecord.src_lng,lastRecord.src_lat);

		double angle=GeometryHelper.GetAngle(last_src_point,point);
		double distance = GeometryHelper.GetDistanceInKm(last_src_point,point);
		double duration = (record_to_match.time.getTime() - lastRecord.match_time.getTime()) / 1000.0;
		double speed = distance / (duration / 3600.0);


		if(duration > 5 && speed > 300){
			//时间间隔大于5秒
			//检测异常点
			//车速异常, 匹配失败
			result = new RoadMatchResult(lng,lat,"IRNORMAL_SPEED");
			return result;
		}


		if(record_to_match.speed < 1.0){
			//判断车辆是否是在停止行驶状态
			long stoptime = 0;

			//如果距离道路距离大于15米
			if(GeometryHelper.GetDistanceInKm(last_src_point,last_point) > 0.015){
				stoptime = 999;
			}else if(GeometryHelper.GetDistanceInKm(point, last_src_point) < 0.010){
				stoptime = lastRecord.stoptime + (long)duration;
			}


			if(stoptime > 0){
				//如果跟上一个点在同一个位置
				result = new RoadMatchResult(
						lng,lat,
						last_point.getX(),last_point.getY(),
						lastRecord.road,
						lastRecord.length_to_road_exit,
						lastRecord.length_to_road_entrance,
						stoptime,
						speed,angle,
						time);
				return result;
			}
		}

		//判断是否还在当前道路上
		if(distance*1.01 < lastRecord.length_to_road_exit && lastRecord.road.angleDiff(angle) < 3.1415926/4){
			Point road_point = lastRecord.road.nearestPointTo(point);
			double road_distance = GeometryHelper.GetDistanceInKm(road_point,point);
			if(road_distance < 0.020){
				match_road = lastRecord.road;
			}
		}

		//多点匹配中方向角的权重
		double angleWeight = 0.0009;

		//判断角度是否发生了较大的变化
		if(match_road==null && history.size() > 1){
			RoadMatchResult last2_record = history.get(index_last-1);
			Point  second_last_src_point = GeometryHelper.CreatePoint(last2_record.src_lng, last2_record.src_lat);
			double angle_diff = GeometryHelper.GetAngleDiff(GeometryHelper.GetAngle(second_last_src_point, last_src_point), angle);
			if(angle_diff > (3.1415927/4)){
				//角度发生较大变化时，方向角权重变小
				angleWeight = 0.0000;
			}
		}

		//如果还没有匹配到道路
		if(match_road == null){
			double min_distance=Double.MAX_VALUE;
			for(Road road : road_list)
			{
				double temp_distance=point.distance(road.geometry)+road.angleDiff(angle,point) * angleWeight / 3.1415927;

				if(temp_distance<min_distance)
				{
					match_road=road;
					min_distance=temp_distance;
				}
			}
		}


		//计算距离道路出口的距离
		Point match_point = match_road.nearestPointTo(point);
		double[] length_to_entrance_and_exit = match_road.lengthToEntranceAndExit(match_point, record_to_match.direction);
		double length_to_entrance = length_to_entrance_and_exit[0];
		double length_to_exit = length_to_entrance_and_exit[1];

		result = new RoadMatchResult(lng,lat,
				match_point.getX(),match_point.getY(),
				match_road,
				length_to_exit,length_to_entrance,
				0L,
				speed,angle,
				time);


		return result;
	}
}
