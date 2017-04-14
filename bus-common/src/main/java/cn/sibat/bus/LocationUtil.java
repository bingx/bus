package cn.sibat.bus;

/**
 * 经纬度工具类
 * Created by kong on 2016/8/18.
 */
public class LocationUtil {
    private static double earth_radius = 6367000;

    /**
     * 计算两个经纬度之间的距离
     *
     * @param lon1 经度1
     * @param lat1 纬度1
     * @param lon2 经度2
     * @param lat2 纬度2
     * @return 距离(m)
     */
    public static double distance(double lon1, double lat1, double lon2, double lat2) {
        double hSinY = Math.sin(toRadians(lat1 - lat2) * 0.5);
        double hSinX = Math.sin(toRadians(lon1 - lon2) * 0.5);
        double s = hSinY * hSinY + Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * hSinX * hSinX;
        return 2 * Math.atan2(Math.sqrt(s), Math.sqrt(1 - s)) * earth_radius;
    }

    /**
     * 转化成弧度制
     *
     * @param d
     * @return
     */
    public static double toRadians(double d) {
        return d * Math.PI / 180;
    }

}
