package gis;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;

import java.io.Serializable;

public class Road implements Serializable {

	public String roadID="";
	public double width;
	public double negative_max_speed;
	public double positive_max_speed;
	public String roadName="";
	public LineString geometry;
	public double angle = 0;
	public boolean isOneWay = false;
	
	public double positive_angle;
	public double negative_angle;
	
	public Road(String roadID, String roadName, double width,double negative_max_speed,double positive_max_speed,LineString geometry)
	{
		this.roadID=roadID;
		this.roadName = roadName;
		this.width=width;
		this.negative_max_speed=negative_max_speed;
		this.positive_max_speed=positive_max_speed;
		this.geometry=geometry;
		
		//计算角度
		for(int i=1;i<this.geometry.getNumPoints();i++)
		{
			angle+=GeometryHelper.GetAngle(this.geometry.getPointN(i-1),this.geometry.getPointN(i));
		}
		angle/=(this.geometry.getNumPoints()-1);

		positive_angle = angle;
		negative_angle = GeometryHelper.ReverseAngle(positive_angle);
		
		isOneWay = (negative_max_speed < 0.1) ^ (positive_max_speed < 0.1);
	}
	
	public double distance(Point point)
	{
		return point.distance(geometry);
	}
	
	public double angleDiff(double another_angle,Point point)
	{
		//比较道路角度差异时，只使用离匹配点较近的几个点
		int n = this.geometry.getNumPoints();
		double angle = this.angle;
		double positive_angle = this.positive_angle;
		double negative_angle = this.negative_angle;
		Point nearestPoint = this.nearestPointTo(point);
		if(n > 2){
			double x = nearestPoint.getX();
			double y = nearestPoint.getY();
			int index = -1;
			for(int i=0;i<n-1;i++){
				Point start = this.geometry.getPointN(i);
				Point end = this.geometry.getPointN(i+1);
				if(between(x,start.getX(),end.getX()) && between(y,start.getY(),end.getY())){
					index = i;
					break;
				}
			}

			int num = 0;
			angle = 0;
			if(index >= 0){
				angle = GeometryHelper.GetAngle(this.geometry.getPointN(index),this.geometry.getPointN(index+1));
//				for(int i=(int)Math.max(0,index-3);i<Math.min(n-1,index+3);i++){
//					angle += GeometryHelper.GetAngle(this.geometry.getPointN(i),this.geometry.getPointN(i+1));
//					num += 1;
//				}
//				angle /= num;

				positive_angle = angle;
				negative_angle = GeometryHelper.ReverseAngle(positive_angle);
			}
		}

		if(isOneWay) 
			return GeometryHelper.GetAngleDiff(another_angle,angle);
		else 
			return Math.min(GeometryHelper.GetAngleDiff(another_angle,positive_angle),GeometryHelper.GetAngleDiff(another_angle,negative_angle));
	}

	public double angleDiff(double another_angle)
	{
		if(isOneWay)
			return GeometryHelper.GetAngleDiff(another_angle,angle);
		else
			return Math.min(GeometryHelper.GetAngleDiff(another_angle,positive_angle),GeometryHelper.GetAngleDiff(another_angle,negative_angle));
	}

	
	public Point nearestPointTo(Point point){
		return GeometryHelper.GetClosestPoint(point,geometry);
	}

	private boolean between(double x,double lower,double upper){
		return (x >= lower && x<=upper) || (x >= upper && x <= lower);
	}
	
	public double[] lengthToEntranceAndExit(Point point, double ahead_angle){
		int n = this.geometry.getNumPoints();
		double x = point.getX();
		double y = point.getY();
		
		double left_distance = 0.0;
		double right_distance = 0.0;
		boolean found = false;
		double[] result = new double[2];

		if(n==2){
			Point start = this.geometry.getStartPoint();
			Point end = this.geometry.getEndPoint();
			left_distance += GeometryHelper.GetDistanceInKm(start, point);
			right_distance += GeometryHelper.GetDistanceInKm(point, end);
			found = true;
		}
		else{
			for(int i=0;i<n-1;i++){
				Point start = this.geometry.getPointN(i);
				Point end = this.geometry.getPointN(i+1);

				if(between(x,start.getX(),end.getX()) && between(y,start.getY(),end.getY())){
					found = true;
					left_distance += GeometryHelper.GetDistanceInKm(start, point);
					right_distance += GeometryHelper.GetDistanceInKm(point, end);
					continue;
				}

				if(found){
					right_distance += GeometryHelper.GetDistanceInKm(start, end);
				}else{
					left_distance += GeometryHelper.GetDistanceInKm(start, end);
				}
			}
		}

		if(found){
			if(this.isOneWay){
				result[0] = left_distance;
				result[1] = right_distance;
			}else{
				double left_angle = GeometryHelper.GetAngleDiff(ahead_angle, this.negative_angle);
				double right_angle = GeometryHelper.GetAngleDiff(ahead_angle, this.positive_angle);
				if(right_angle <= left_angle){
					result[0] = left_distance;
					result[1] = right_distance;
				}else{
					result[1] = left_distance;
					result[0] = right_distance;
				}
			}
		}
		return result;
	}
}
