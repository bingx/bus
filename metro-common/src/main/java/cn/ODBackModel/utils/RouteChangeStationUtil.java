package cn.ODBackModel.utils;

import cn.ODBackModel.mainPkg.readForm.MapAllPath;
import cn.ODBackModel.tableTable.TimeTableReader;

import java.util.List;
import java.util.Map;

/**
 * Created by hu on 2016/11/3.
 *
 *  路径 和 换乘站路径  相互转换
 *  route:  45-46-47-48-49-50-60-61-62-63-64-28
 *  changeStations:1260026000-1268003000-1261006000
 */
public class RouteChangeStationUtil {

    /**
     *
     *  Map<String,String> lineStation2station =TimeTableReader.getLineStation2station();
     *
     *  45-46-47-48-49-50-60-61-62-63-64-28
     *  1260026000-1268003000-1261006000
     *
     *
     */
    public static String Route2ChangeStation(String route,Map<String,String> lineStation2station){

        String[] serStations = route.split("-");
        String changeStations = NoNameExUtil.Ser2NO(serStations[0])+"-"+NoNameExUtil.Ser2NO(serStations[serStations.length-1]);

        for(int i=1; i<serStations.length -1; i++){
            String theStation = NoNameExUtil.Ser2NO(serStations[i]); //当前站点
            String preStation = NoNameExUtil.Ser2NO(serStations[i-1]); //上一个站点
            String nextStation = NoNameExUtil.Ser2NO(serStations[i+1]); //下一个站点

            String preLine = lineStation2station.get(preStation+theStation); //上一条路线
            String nextLine = lineStation2station.get(theStation+nextStation); //下一条路线
            //如果上一条路线与下一条路线不一致，则为当前站点为转乘站
            if (!preLine.equals(nextLine)){
                changeStations = changeStations + "-" + theStation; //O-D-换乘站
            }
        }
        return changeStations;
    }

    /**
     * 有误差
     * 所有路径里面可以能不包括此换乘路径，因为实际乘车多选择换乘少的路径，不看时间
     * 1260026000-1268003000-1261006000
     * 45-46-47-48-49-50-60-61-62-63-64-28
     */
    public static String ChangeStation2Route(String changeStations, Map<String,List<String>> mapAllPath, Map<String,String> lineStation2station){

        String route = null;

        String[] stations = changeStations.split("-");
        String ODKey = NoNameExUtil.NO2Ser(stations[0]) + "-" + NoNameExUtil.NO2Ser(stations[1]);
        //如果路径表中拥有OD对应的路径列表，则将路径列表转换为route
        if (mapAllPath.containsKey(ODKey)){

            List<String> routesList =mapAllPath.get(ODKey);

            for(int i=0; i<routesList.size(); i++){
                String serRoute =routesList.get(i).split("#")[0].replaceAll(" ","-");
                serRoute =serRoute.substring(0,serRoute.length() -1);

                String routeChangeStations =Route2ChangeStation(serRoute, lineStation2station);
                if (routeChangeStations.equals(changeStations)){
//                    route =serRoute.replaceAll(",","-");
                    route = serRoute;
                    break;
                }
            }
            /**
             * allPathNew8.txt中可能没有此换乘路径
             */
            if(route == null){

            }
        }
        return route;
    }

    public static void main(String[] args) {
        //输出相邻站编码分别为1262019000和1267022000的路线编码输出
        System.out.println(TimeTableReader.getLineStation2station().get("12620190001267022000"));
        //将路线转换为转乘站路线测试
        Map<String,String> lineStation2station =TimeTableReader.getLineStation2station();
        String changeStations =Route2ChangeStation("45 46 47 48 49 50 60 61 62 63 64 28".replace(" ","-"), lineStation2station);
        System.out.println("changeStations: " + changeStations);
        System.out.println("------------------");
        //将转乘站路线转换成完整乘车路线测试
        Map<String,List<String>> mapAllPath = MapAllPath.getAllPath();
        String route =ChangeStation2Route("1260026000-1268003000-1261006000", mapAllPath, lineStation2station);
        System.out.println("route: "+route);
        System.out.println("--------------------");
    }
}
