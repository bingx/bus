package gis;

import com.vividsolutions.jts.algorithm.Angle;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import org.geotools.geometry.jts.JTSFactoryFinder;

public class GeometryHelper {
	private static double EARTH_RADIUS = 6378.137;
	

	public static double rad(double d) {
		return d * Math.PI / 180.0;
	}
	
	public static double RadToAngle(double angle){
		return angle;
		//return angle * 180.0 / Math.PI;
	}
	
	public static GeometryFactory geometryFactory  = JTSFactoryFinder.getGeometryFactory();
	
	public static Point GetClosestPoint(Point point , LineString road)
	{
		return geometryFactory.createPoint(DistanceOp.closestPoints(road,point)[0]);
	}
	
	public static LineString CreateLine(double x1,double y1,double x2,double y2)
	{
		Coordinate[] array=new Coordinate[]{new Coordinate(x1,y1),new Coordinate(x2,y2)};
		return geometryFactory.createLineString(array);
	}
	
	public static LineString CreateLine( Point p1, Point p2)
	{
		Coordinate[] array=new Coordinate[]{p1.getCoordinate(),p2.getCoordinate()};
		return geometryFactory.createLineString(array);
	}
	
	public static double GetDistanceInKm(Point p1,Point p2)
	{
		return  GetDistance(p1.getY(),p1.getX(),p2.getY(),p2.getX())/1000;
	}
	
	public static double GetDistanceInKm(double lng1,double lat1,double lng2,double lat2){
		return GetDistance(lat1, lng1, lat2, lng2)/1000;
	}
	
	
	//GetDistance in meters,(m)
	public static double GetDistance(double lat1, double lng1, double lat2,
			double lng2) {
		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = s * 10000000 / 10000;
		return s;
	}
	
	public static double GetAngle(Point p1, Point p2)
	{
		return Angle.normalizePositive(Angle.angle(p1.getCoordinate(),p2.getCoordinate()));
	}
	
	public static double ReverseAngle(double angle)
	{
		return Angle.normalizePositive(angle - Math.PI);
	}
	
	public static double GetAngleDiff(double angle1,double angle2)
	{
		return Angle.diff(angle1,angle2);
	}
	
	public static class Pos {
		public double x;
		public double y;

		public Pos(double n1, double n2) {
			x = n1;
			y = n2;
		}
	}
	
	public static double GetDistance(Pos p1, Pos p2) {
		return GetDistance(p1.y, p1.x, p2.y, p2.x);
	}
	
	public static Point CreatePoint(double lng,double lat){
		return geometryFactory.createPoint(new Coordinate(lng,lat));
	}

}
