package cn.sibat.bus;

/**
 * 经纬度工具类
 * Created by kong on 2016/8/18.
 */
public class LocationUtil {
    private static double EARTH_RADIUS = 6378137;
    private static double ee = 0.00669342162296594323;

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
        return 2 * Math.atan2(Math.sqrt(s), Math.sqrt(1 - s)) * EARTH_RADIUS;
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

    /**
     * 84 to 火星坐标系 (GCJ-02) World Geodetic System ==> Mars Geodetic System
     *
     * @param lat 84纬度
     * @param lon 84经度
     * @return gcj s'(lat,lon)
     */
    public static String gps84_To_Gcj02(double lat, double lon) {
        double dLat = transformLat(lon - 105.0, lat - 35.0);
        double dLon = transformLon(lon - 105.0, lat - 35.0);
        double radLat = lat / 180.0 * Math.PI;
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((EARTH_RADIUS * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
        dLon = (dLon * 180.0) / (EARTH_RADIUS/ sqrtMagic * Math.cos(radLat) * Math.PI);
        double mgLat = lat + dLat;
        double mgLon = lon + dLon;
        return mgLat + "," + mgLon;
    }

    /**
     * 火星坐标系 (GCJ-02) 与百度坐标系 (BD-09) 的转换算法 将 GCJ-02 坐标转换成 BD-09 坐标
     *
     * @param gg_lat 火星纬度
     * @param gg_lon 火星经度
     * @return bd s'(lat,lon)
     */
    public static String gcj02_To_Bd09(double gg_lat, double gg_lon) {
        double z = Math.sqrt(gg_lon * gg_lon + gg_lat * gg_lat) + 0.00002 * Math.sin(gg_lat * Math.PI);
        double theta = Math.atan2(gg_lat, gg_lon) + 0.000003 * Math.cos(gg_lon * Math.PI);
        double bd_lon = z * Math.cos(theta) + 0.0065;
        double bd_lat = z * Math.sin(theta) + 0.006;
        return bd_lat + "," + bd_lon;
    }

    /**
     * 84转换成百度坐标系(BD-09)
     *
     * @param lat 84纬度
     * @param lon 84经度
     * @return bd s'(lat,lon)
     */
    public static String gps84_To_Bd09(double lat, double lon) {
        String gps84_to_gcj02 = gps84_To_Gcj02(lat, lon);
        return gcj02_To_Bd09(Double.parseDouble(gps84_to_gcj02.split(",")[0]), Double.parseDouble(gps84_to_gcj02.split(",")[1]));
    }

    /**
     * 火星坐标系（GCJ-02）转换成84
     *
     * @param lat 火星纬度
     * @param lon 火星经度
     * @return 84 s'(lat,lon)
     */
    public static String gcj02_To_84(double lat, double lon) {
        String gps = transform(lat, lon);
        double lon_84 = lon * 2 - Double.parseDouble(gps.split(",")[1]);
        double lat_84 = lat * 2 - Double.parseDouble(gps.split(",")[0]);
        return lat_84 + "," + lon_84;
    }

    /**
     * 百度坐标系（bd09）转换成火星坐标系（gcj02）
     *
     * @param bd_lat 百度纬度
     * @param bd_lon 百度经度
     * @return gcj s'(lat,lon)
     */
    public static String bd09_To_gcj02(double bd_lat, double bd_lon) {
        double x = bd_lon - 0.0065, y = bd_lat - 0.006;
        double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * Math.PI);
        double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * Math.PI);
        double gg_lon = z * Math.cos(theta);
        double gg_lat = z * Math.sin(theta);
        return gg_lat + "," + gg_lon;
    }

    /**
     * 百度坐标系（bd09）转成84
     *
     * @param bd_lat 百度纬度
     * @param bd_lon 百度经度
     * @return 84 s'(lat,lon)
     */
    public static String bd09_To_84(double bd_lat, double bd_lon) {
        String gcj02 = bd09_To_gcj02(bd_lat, bd_lon);
        return gcj02_To_84(Double.parseDouble(gcj02.split(",")[0]), Double.parseDouble(gcj02.split(",")[1]));
    }

    private static String transform(double lat, double lon) {
        double dLat = transformLat(lon - 105.0, lat - 35.0);
        double dLon = transformLon(lon - 105.0, lat - 35.0);
        double radLat = toRadians(lat);
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((EARTH_RADIUS * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
        dLon = (dLon * 180.0) / (EARTH_RADIUS / sqrtMagic * Math.cos(radLat) * Math.PI);
        double mgLon = lon + dLon;
        double mgLat = lat + dLat;
        return mgLat + "," + mgLon;
    }

    private static double transformLat(double x, double y) {
        double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y
                + 0.2 * Math.sqrt(Math.abs(x));
        ret += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(y * Math.PI) + 40.0 * Math.sin(y / 3.0 * Math.PI)) * 2.0 / 3.0;
        ret += (160.0 * Math.sin(y / 12.0 * Math.PI) + 320 * Math.sin(y * Math.PI / 30.0)) * 2.0 / 3.0;
        return ret;
    }

    private static double transformLon(double x, double y) {
        double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1
                * Math.sqrt(Math.abs(x));
        ret += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(x * Math.PI) + 40.0 * Math.sin(x / 3.0 * Math.PI)) * 2.0 / 3.0;
        ret += (150.0 * Math.sin(x / 12.0 * Math.PI) + 300.0 * Math.sin(x / 30.0
                * Math.PI)) * 2.0 / 3.0;
        return ret;
    }
}
