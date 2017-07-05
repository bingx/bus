package cn.ODBackModel.utils;

/**
 * Created by hu on 2016/11/3.
 */
public class RouteNameNoSerUtil {

    /**
     *  路径的 站点名称-站点编号 转化
     *  1268032000-1268030000-1268028000-1268024000
     */
    public static String RouteName2No(String string){

        String[] stations =string.split("-");

        StringBuilder sb =new StringBuilder();
        for(int i =0;i<stations.length;i++){
            if (i ==stations.length -1){
                sb.append(NoNameExUtil.Name2NO(stations[i]));
            }else {
                sb.append(NoNameExUtil.Name2NO(stations[i])+"-");
            }
        }
        return sb.toString();
    }

    /**
     *  路径的 站点编号-站点名称 转化
     */
    public static String RouteNo2Name(String string){

        String[] stations =string.split("-");

        StringBuilder sb =new StringBuilder();
        for(int i =0;i<stations.length;i++){
            if (i ==stations.length -1){
                sb.append(NoNameExUtil.NO2Name(stations[i]));
            }else {
                sb.append(NoNameExUtil.NO2Name(stations[i])+"-");
            }
        }
        return sb.toString();
    }

    /**
     * 路径的 站点编号-站点序号 转化
     */
    public static String RouteNo2Ser(String string){
        String[] stations =string.split("-");

        StringBuilder sb =new StringBuilder();
        for(int i =0;i<stations.length;i++){
            if (i ==stations.length -1){
                sb.append(NoNameExUtil.NO2Ser(stations[i]));
            }else {
                sb.append(NoNameExUtil.NO2Ser(stations[i])+"-");
            }
        }
        return sb.toString();
    }

    /**
     *  路径的 站点序号-站点编号 转化
     */
    public static String RouteSer2No(String string){
        String[] stations =string.split("-");

        StringBuilder sb =new StringBuilder();
        for(int i =0;i<stations.length;i++){
            if (i ==stations.length -1){
                sb.append(NoNameExUtil.Ser2NO(stations[i]));
            }else {
                sb.append(NoNameExUtil.Ser2NO(stations[i])+"-");
            }
        }
        return sb.toString();
    }

    /**
     *  路径的 站点序号-站点名称 转化
     */
    public static String RouteSer2Name(String string){
        String[] stations =string.split("-");

        StringBuilder sb =new StringBuilder();
        for(int i =0;i<stations.length;i++){
            if (i ==stations.length -1){
                sb.append(NoNameExUtil.Ser2Name(stations[i]));
            }else {
                sb.append(NoNameExUtil.Ser2Name(stations[i])+"-");
            }
        }
        return sb.toString();
    }

    /**
     *  路径的 站点名称-站点序号 转化
     */
    public static String RouteName2Ser(String string){
        String[] stations =string.split("-");

        StringBuilder sb =new StringBuilder();
        for(int i =0;i<stations.length;i++){
            if (i ==stations.length -1){
                sb.append(NoNameExUtil.Name2Ser(stations[i]));
            }else {
                sb.append(NoNameExUtil.Name2Ser(stations[i])+"-");
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("坪洲-宝安中心-前海湾-深大:"+RouteName2No("坪洲-宝安中心-前海湾-深大"));
        System.out.println("1268032000-1268030000-1268028000-1268024000:"+RouteNo2Name("1268032000-1268030000-1268028000-1268024000"));
        System.out.println("45-46-47-48-49-50-60-61-62-63-64-28:"+RouteSer2Name("45-46-47-48-49-50-60-61-62-63-64-28"));
    }
}
