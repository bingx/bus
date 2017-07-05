package cn.ODBackModel.mainPkg;

import cn.ODBackModel.mainPkg.readForm.MapAllPath;
import cn.ODBackModel.subwayPkg.pojo.BaseSubway;
import cn.ODBackModel.tableTable.TimeTableMaker;
import cn.ODBackModel.tableTable.TimeTableReader;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import static cn.ODBackModel.mainPkg.readForm.MapTimeWalk.getMapWalk;

import cn.ODBackModel.utils.CodingDetector;
import cn.ODBackModel.utils.NoNameExUtil;
import cn.ODBackModel.utils.RouteChangeStationUtil;
import cn.ODBackModel.utils.TimeConvert;
import cn.sibat.metroUtils.TimeUtils;

/**
 * Created by hu on 2016/11/3.
 * Updated by wing on 2017/6/3
 * OD反推
 */
public class MainThread {
    //全局变量
    private static Map<String, String> lineStation2station = TimeTableReader.getLineStation2station();
    private static Map<String, String> linesO2D = TimeTableReader.getLinesO2D();
    private static Map<String, List<BaseSubway>> lbs = TimeTableMaker.getSubwayList();
    private static Map<String, String> mapWalk = getMapWalk();

    private static Integer count = 0;//计算路径成功匹配的记录
    //测试
    public static void main(String[] args) throws IOException, ParseException {

        //为了计算运行程序所用时间
        Long start = System.currentTimeMillis();

        String path = "metro-common/src/main/resources/testData";
        Scanner scan = new Scanner(new FileInputStream(new File(path)), CodingDetector.checkTxtCode(path));

        String path3 = "result_20170102"; //输出
        BufferedWriter bw = new BufferedWriter(new FileWriter(path3));

        while (scan.hasNext()) {
            String line = scan.nextLine();
            String[] strings = line.split(",");
            String cardNo = strings[0];

            String matchedLine = CheckAllPathNew(strings);

            if (matchedLine != null) {
                count ++;
                String result = functionPrintData(cardNo, matchedLine);
                System.out.println(result);
                bw.write(result);
                bw.newLine();
                bw.flush();
            } else System.out.println("match failure!");
        }
        System.out.println("count:" + count);
        System.out.println("time:" + (System.currentTimeMillis() - start) / 1000 / 60.0 + "min"); //程序运行时间
        bw.close();
    }

    /**
     * OD反推核心算法
     * @param strings   OD记录
     * @return matchedLine 匹配的路径
     */
    public static String CheckAllPathNew(String[] strings) throws ParseException {

        String matchedLine = null;
        Boolean flagIsChange = false;

        Integer oneChangeNo = 0; //一次换乘
        Integer twoChangeNo = 0;
        Integer threeChangeNo = 0;
        Integer fourChangeNo = 0;

        Integer IN_STATION = 150;
        Integer OUT_STATION = 90;

        Date dayDate = TimeConvert.String2DayDate(strings[strings.length-2]); //天

        long inStationTime = TimeUtils.apply().time2stamp(strings[3], "yyyy-MM-dd HH:mm:ss");
        long outStationTime = TimeUtils.apply().time2stamp(strings[10], "yyyy-MM-dd HH:mm:ss");
        long upSubwayTime = inStationTime + IN_STATION;
        long downSubwayTime = outStationTime - OUT_STATION;
        double realTime = (outStationTime - inStationTime) / 60.0; //分钟

        //将OD站点名称转化为序号
        String string_O2;
        String string_D2;
        string_O2 = NoNameExUtil.Name2Ser(strings[5]);//"黄贝岭"-->56
        string_D2 = NoNameExUtil.Name2Ser(strings[12]);//"新秀"-->57

        String key = string_O2 + "-" + string_D2;//OD key
        List<String> value = MapAllPath.getAllPath().get(key);//路径list

        String closeLine = value.get(0); //初始化
        String[] timeList = closeLine.split("#")[1].split(" ");
        Double closeTime = Double.parseDouble(timeList[timeList.length - 1]); //初始化

        List<Integer> changeTimeList = countChangeTimes(value);

        //若只有一条路径，则直接输出
        if (value.size() == 1) {
            matchedLine = closeLine;
            return matchedLine;
        }

        //若有多条路径，则进入循环判断
        int valueNo;
        for (valueNo = 0; valueNo < value.size(); valueNo++) {

            String thisLine = value.get(valueNo); //当前路径
            String[] strings_allPath = thisLine.split("#");//56 57 #0 T 0.00000000  1.76666667
            String[] stations = value.get(valueNo).split("#")[0].split(" "); //路径列表
            String[] pathTime = strings_allPath[1].split(" "); //换乘次数, 换乘时间以及乘车时间组成的列表[0, T, 0.00000000, 1.76666667]
            String lineO = lineStation2station.get(NoNameExUtil.Ser2NO(stations[0]) + NoNameExUtil.Ser2NO(stations[1])); //上车所乘路线
            Double time = Double.parseDouble(pathTime[pathTime.length - 1]); //路径花费时间
            String transferTime = pathTime[0]; //换乘次数

            /*
              若OD在同一条线路上
                   若有直达的路径
                       满足直达条件：输出直达路径
                       不满足直达条件：继续下一直达路径循环判断
                   没有直达路径
                       输出时间最接近的路线
             */
            String lineNo = linesO2D.get(NoNameExUtil.Ser2NO(string_O2) + NoNameExUtil.Ser2NO(string_D2)); //路线编码
            if (lineNo != null) { //如果OD共线路
                if (transferTime.equals("0")) { //若有直达路径
                    Boolean flagIsOneNo = ODPathBack_isOneNo(lineO, strings, upSubwayTime, downSubwayTime, dayDate);
                    if (flagIsOneNo) { //若该直达路径满足直达条件
                        matchedLine = value.get(valueNo);
                        break;
                    }
                } else { //若没有直达路径
                    matchedLine = CloseTimeLine(value, realTime);
                    break;
                }
            } else {

                /*
                  若OD不在同一条线路上
                   O点在哪一条线上，D点在哪一条线上                     --------处理换乘代码
                   有i次换乘的路径 --遍历所有
                       有满足条件的换乘路径：输出满足i次换乘条件且时间最接近的那条路径
                       没有满足条件的换乘路径：继续循环判断直到最后一条
                                           当前路径为最后一条且没有满足添加的换乘路径：输出时间上最接近的路径
                 */
                Boolean flagChange;
                String changeStations = RouteChangeStationUtil.Route2ChangeStation(thisLine.split("#")[0].replaceAll(" ", "-"), lineStation2station);
                 try {
                     flagChange = ODPathBackIsChange(stations, changeStations, transferTime, upSubwayTime, downSubwayTime, dayDate);
                 }
                 catch (NullPointerException e) {
                     System.out.println("亲爱哒，你的程序出Bug啦! OD记录的卡号为：" + strings[0]);
                     break;
                 }

                if (transferTime.equals("1")) {
                    oneChangeNo++;
                    if (flagChange) { //如果都满足一次换乘，找出时间上最接近的路径
                        if (time <= realTime && time >= closeTime) {
                            flagIsChange = true;
                            closeTime = time;
                            closeLine = thisLine;
                        }
                        if (Objects.equals(oneChangeNo, changeTimeList.get(0))) {//若当前的换乘数目路径为最后一条一次换乘路径
                            matchedLine = closeLine;
                            break;
                        }
                    } else if (!flagIsChange && valueNo == value.size() - 1) {//若仍没有匹配的路径则输入时间上最接近的一条路径
                        //System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
                        break;
                    } else if (flagIsChange && Objects.equals(oneChangeNo, changeTimeList.get(0))) { //若当前路径不能匹配但是有匹配的路径则直接输出最接近的路径
                        matchedLine = closeLine;
                        break;
                    }

                } else if (transferTime.equals("2")) {
                    twoChangeNo++;
                    if (flagChange) {
                        if (time <= realTime && time >= closeTime) {
                            flagIsChange = true;
                            closeTime = time;
                            closeLine = thisLine;
                        }
                        if (Objects.equals(twoChangeNo, changeTimeList.get(1))) {
                            matchedLine = closeLine;
                            break;
                        }
                    } else if (!flagIsChange && valueNo == value.size() - 1) {//若仍没有匹配的路径则输入时间上最接近的一条路径
                        //System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
                        break;
                    } else if (flagIsChange && Objects.equals(twoChangeNo, changeTimeList.get(1))) {
                        matchedLine = closeLine;
                        break;
                    }

                } else if (transferTime.equals("3")) {
                    threeChangeNo++;
                    if (flagChange) {
                        if (time <= realTime && time >= closeTime) {
                            flagIsChange = true;
                            closeTime = time;
                            closeLine = thisLine;
                        }
                        if (Objects.equals(threeChangeNo, changeTimeList.get(2))) {
                            matchedLine = closeLine;
                            break;
                        }
                    } else if (!flagIsChange && valueNo == value.size() - 1) {//若仍没有匹配的路径则输入时间上最接近的一条路径
                        //System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
                        break;
                    } else if (flagIsChange && Objects.equals(threeChangeNo, changeTimeList.get(2))) {
                        matchedLine = closeLine;
                        break;
                    }

                } else if (transferTime.equals("4")) {
                    fourChangeNo++;
                    if (flagChange) {
                        if (time <= realTime && time >= closeTime) {
                            flagIsChange = true;
                            closeTime = time;
                            closeLine = thisLine;
                        }
                        if (Objects.equals(fourChangeNo, changeTimeList.get(3))) {
                            matchedLine = closeLine;
                            break;
                        }
                    } else if (!flagIsChange && valueNo == value.size() - 1) {//若仍没有匹配的路径则输入时间上最接近的一条路径
                        //System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
                        break;
                    } else if (flagIsChange && Objects.equals(fourChangeNo, changeTimeList.get(3))) {
                        matchedLine = closeLine;
                        break;
                    }
                } else { //对于4次以上的换乘直接输出时间上最接近的路径
                    matchedLine = CloseTimeLine(value, realTime);
                    break;
                }
            }
        }
        return matchedLine;
    }

    /**
     * 匹配成功下的结果输出
     *
     * @param cardNo  卡号
     * @param strings 路径
     * @return stringCon 组合结果
     */
    public static String functionPrintData(String cardNo, String strings) {

        String stringCon = ConvertOD(strings);
        stringCon = cardNo + "," + stringCon;
        return stringCon;
    }

    /**
     * 转化为最后的结果，每个站点以字符串表示
     */
    private static String ConvertOD(String string) {

        String[] strings = string.split("#")[0].split(" ");
        StringBuilder sb = new StringBuilder();
        for (String i : strings) {
            sb.append(NoNameExUtil.Ser2Name(i));
            sb.append("-");
        }
        return sb.toString().substring(0, sb.toString().length()-1);
    }

    /**
     * 判断是否满足换乘条件
     *
     * @param stations       站点列表
     * @param changeStations 换乘站点
     * @param transferTime   换乘次数
     * @return flag 是否满足换乘条件
     */
    private static boolean ODPathBackIsChange(String[] stations, String changeStations, String transferTime, Long upSubwayTime, Long downSubwayTime, Date dayDate) throws ParseException {

        boolean flag = false;

        String[] odChangeStations = changeStations.split("-");
        String string_O2 = odChangeStations[0]; //起点站序号
        String string_D2 = odChangeStations[1]; //终点站序号

        Date upSubwayDate = TimeUtils.apply().stamp2Date(upSubwayTime);
        Date downSubwayDate = TimeUtils.apply().stamp2Date(downSubwayTime);

        String station2stationFirst = NoNameExUtil.Ser2NO(stations[0]) + NoNameExUtil.Ser2NO(stations[1]);
        String lineNoFirst = lineStation2station.get(station2stationFirst); //上车线路
        String station2stationLast = NoNameExUtil.Ser2NO(stations[stations.length - 2]) + NoNameExUtil.Ser2NO(stations[stations.length - 1]);
        String lineNoLast = lineStation2station.get(station2stationLast); //下车线路

        switch (transferTime) {
            case "1": {// #1
                String changeStation = odChangeStations[2]; //换乘站编码

                //到达换乘站的时刻
                Date timed_change = timed_change(lineNoFirst, string_O2, upSubwayDate, changeStation, dayDate);
                //从换乘站出发的时刻
                Date timeo_change = timeo_change(lineNoLast, string_D2, downSubwayDate, changeStation, dayDate);
                //该站点最小换乘时间
                int CHANGE_STATION_SMALL = change_station_walkTime(changeStation, lineNoFirst, lineNoLast);

                if (TimeUtils.apply().date2Stamp(timeo_change) - TimeUtils.apply().date2Stamp(timed_change) >= CHANGE_STATION_SMALL) {
                    flag = true;
                }
                break;
            }
            case "2": {//#2
                String changeStation1 = odChangeStations[2];
                String changeStation2 = odChangeStations[3];

                String stationNext = null;
                //第一个换乘站的下一站
                for (int i = 1; i < stations.length; i++) {
                    if (stations[i - 1].equals(NoNameExUtil.NO2Ser(changeStation1))) {
                        stationNext = NoNameExUtil.Ser2NO(stations[i]);
                        break;
                    }
                }

                String station2stationSecond = changeStation1 + stationNext;
                String lineNo_Second = lineStation2station.get(station2stationSecond); //在第一个换乘站所乘线路

                Date timed_change_first = timed_change(lineNoFirst, string_O2, upSubwayDate, changeStation1, dayDate);
                int CHANGE_STATION_SMALL1 = change_station_walkTime(changeStation1, lineNoFirst, lineNo_Second);
                //由最短换乘时间计算乘客在第一个换乘站的出发时间
                Date timeo_change_first = TimeUtils.apply().stamp2Date(TimeUtils.apply().date2Stamp(timed_change_first) + CHANGE_STATION_SMALL1);

                //由第一个换乘站出发的时间---timeo_change_first找到第二个换乘站到达的时间---timed_change_second
                Date timed_change_last = timed_change(lineNo_Second, changeStation1, timeo_change_first, changeStation2, dayDate);
                int CHANGE_STATION_SMALL_LAST = change_station_walkTime(changeStation2, lineNo_Second, lineNoLast);

                //由到达终点站的时间反推最后一个换乘点出发的时间---timeo_change_last
                Date timeo_change_last = timeo_change(lineNoLast, string_D2, downSubwayDate, changeStation2, dayDate);

                if (TimeUtils.apply().date2Stamp(timeo_change_last) - TimeUtils.apply().date2Stamp(timed_change_last) >= CHANGE_STATION_SMALL_LAST) {
                    flag = true;
                }
                break;
            }
            case "3": {// #3
                String changeStation1 = odChangeStations[2];
                String changeStation2 = odChangeStations[3];
                String changeStation3 = odChangeStations[4];

                String stationNext1 = null;
                String stationNext2 = null;
                for (int i = 1; i < stations.length; i++) {
                    if (stations[i - 1].equals(NoNameExUtil.NO2Ser(changeStation1))) {
                        stationNext1 = NoNameExUtil.Ser2NO(stations[i]);
                    }
                    if (stations[i - 1].equals(NoNameExUtil.NO2Ser(changeStation2))) {
                        stationNext2 = NoNameExUtil.Ser2NO(stations[i]);
                    }
                }

                String station2stationSecond = changeStation1 + stationNext1;
                String lineNo_Second = lineStation2station.get(station2stationSecond);
                String station2stationThird = changeStation2 + stationNext2;
                String lineNo_Third = lineStation2station.get(station2stationThird);

                Date timed_change_first = timed_change(lineNoFirst, string_O2, upSubwayDate, changeStation1, dayDate);
                int CHANGE_STATION_SMALL1 = change_station_walkTime(changeStation1, lineNoFirst, lineNo_Second);
                Date timeo_change_first = TimeUtils.apply().stamp2Date(TimeUtils.apply().date2Stamp(timed_change_first) + CHANGE_STATION_SMALL1);

                Date timed_change_second = timed_change(lineNo_Second, changeStation1, timeo_change_first, changeStation2, dayDate);
                int CHANGE_STATION_SMALL2 = change_station_walkTime(changeStation2, lineNo_Second, lineNo_Third);
                Date timeo_change_second = TimeUtils.apply().stamp2Date(TimeUtils.apply().date2Stamp(timed_change_second)+ CHANGE_STATION_SMALL2);

                Date timed_change_last = timed_change(lineNo_Third, changeStation2, timeo_change_second, changeStation3, dayDate);
                int CHANGE_STATION_SMALL_LAST = change_station_walkTime(changeStation3, lineNo_Third, lineNoLast);
                Date timeo_change_last = timeo_change(lineNoLast, string_D2, downSubwayDate, changeStation3, dayDate);

                if (TimeUtils.apply().date2Stamp(timeo_change_last) - TimeUtils.apply().date2Stamp(timed_change_last) >= CHANGE_STATION_SMALL_LAST) {
                    flag = true;
                }
                break;
            }
            case "4": {// #4
                String changeStation1 = odChangeStations[2];
                String changeStation2 = odChangeStations[3];
                String changeStation3 = odChangeStations[4];
                String changeStation4 = odChangeStations[5];

                String stationNext1 = null;
                String stationNext2 = null;
                String stationNext3 = null;
                for (int i = 1; i < stations.length; i++) {
                    if (stations[i - 1].equals(NoNameExUtil.NO2Ser(changeStation1))) {
                        stationNext1 = NoNameExUtil.Ser2NO(stations[i]);
                    }
                    if (stations[i - 1].equals(NoNameExUtil.NO2Ser(changeStation2))) {
                        stationNext2 = NoNameExUtil.Ser2NO(stations[i]);
                    }
                    if (stations[i - 1].equals(NoNameExUtil.NO2Ser(changeStation3))) {
                        stationNext3 = NoNameExUtil.Ser2NO(stations[i]);
                    }
                }

                String station2stationSecond = changeStation1 + stationNext1;
                String lineNo_Second = lineStation2station.get(station2stationSecond);
                String station2stationThird = changeStation2 + stationNext2;
                String lineNo_Third = lineStation2station.get(station2stationThird);
                String station2stationForth = changeStation3 + stationNext3;
                String lineNo_Forth = lineStation2station.get(station2stationForth);

                Date timed_change_first = timed_change(lineNoFirst, string_O2, upSubwayDate, changeStation1, dayDate);
                int CHANGE_STATION_SMALL1 = change_station_walkTime(changeStation1, lineNoFirst, lineNo_Second);
                Date timeo_change_first = TimeUtils.apply().stamp2Date(TimeUtils.apply().date2Stamp(timed_change_first) + CHANGE_STATION_SMALL1);

                Date timed_change_second = timed_change(lineNo_Second, changeStation1, timeo_change_first, changeStation2, dayDate);
                int CHANGE_STATION_SMALL2 = change_station_walkTime(changeStation2, lineNo_Second, lineNo_Third);
                Date timeo_change_second = TimeUtils.apply().stamp2Date(TimeUtils.apply().date2Stamp(timed_change_second) + CHANGE_STATION_SMALL2);

                Date timed_change_third = timed_change(lineNo_Third, changeStation2, timeo_change_second, changeStation3, dayDate);
                int CHANGE_STATION_SMALL3 = change_station_walkTime(changeStation3, lineNo_Third, lineNo_Forth);
                Date timeo_change_third = TimeUtils.apply().stamp2Date(TimeUtils.apply().date2Stamp(timed_change_third) + CHANGE_STATION_SMALL3);

                Date timed_change_last = timed_change(lineNo_Forth, changeStation3, timeo_change_third, changeStation4,dayDate);
                int CHANGE_STATION_SMALL_LAST = change_station_walkTime(changeStation4, lineNo_Forth, lineNoLast);
                Date timeo_change_last = timeo_change(lineNoLast, string_D2, downSubwayDate, changeStation4,dayDate);

                if (TimeUtils.apply().date2Stamp(timeo_change_last) - TimeUtils.apply().date2Stamp(timed_change_last) >= CHANGE_STATION_SMALL_LAST) {
                    flag = true;
                }
                break;
            }
        }
        return flag;
    }

    /**
     * 判断直达路径上的列车号是否一致用于直达条件 #0
     *
     * @param lineO   列车所在线路
     * @param strings OD记录
     * @return flag
     * @throws ParseException 时间解析异常
     */
    private static Boolean ODPathBack_isOneNo(String lineO, String[] strings, Long upSubwayTime, Long downSubwayTime, Date dayDate) throws ParseException {

        Boolean flag = false;

        String string_O = NoNameExUtil.Name2NO(strings[6]); //O站点编码
        String string_D = NoNameExUtil.Name2NO(strings[14]); //D站点编码
        Integer subwayNo_O = 0;
        Integer subwayNo_D = 0;

        Date upSubwayDate = TimeUtils.apply().stamp2Date(upSubwayTime);
        Date downSubwayDate = TimeUtils.apply().stamp2Date(downSubwayTime);

        List<BaseSubway> lineTimeTable = lbs.get(lineO); //当前线路上的列车运行时刻表
        for (int subwayNo = 0; subwayNo < lineTimeTable.size(); subwayNo++) {
            for (int stationNo = 0; stationNo < lineTimeTable.get(subwayNo).getStations().size(); stationNo++) {
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_O)) {
                    subwayNo_O = matchSubwayNo(lineO, subwayNo, stationNo, upSubwayDate, dayDate);
                }
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_D)) {
                    subwayNo_D = matchSubwayNo(lineO, subwayNo, stationNo, downSubwayDate, dayDate);
                }
            }
            if (subwayNo_O != null && subwayNo_D != null) {
                if (subwayNo_O.equals(subwayNo_D)) {
                    flag = true;
                }
                break;
            }
        }
        return flag;
    }

    /**
     * 不同换乘站点不通路线间的最小换乘时间
     *
     * @param changeStation 换乘站
     * @param lineNo_O      换乘前的线路
     * @param lineNo_D      换乘后的线路
     * @return CHANGE_STATION_SMALL                 此次换乘用的最短步行时间
     */
    private static int change_station_walkTime(String changeStation, String lineNo_O, String lineNo_D) {

        int CHANGE_STATION_SMALL = 0;
        String upLine = "0"; //上行
        String downLine = "1"; //下行
        String key = changeStation + "," + lineNo_O.substring(0, 3) + "," + lineNo_D.substring(0, 3); //换乘站点
        String value = mapWalk.get(key);
        //上行转上行，上行转下行，下行转上行，下行转下行，且换乘分钟转为秒
        if (lineNo_O.substring(3).equals(upLine) && lineNo_D.substring(3).equals(upLine)) {
            CHANGE_STATION_SMALL = (int) ((Double.parseDouble(value.split(",")[0])) * 60);
        } else if (lineNo_O.substring(3).equals(upLine) && lineNo_D.substring(3).equals(downLine)) {
            CHANGE_STATION_SMALL = (int) ((Double.parseDouble(value.split(",")[1])) * 60);
        } else if (lineNo_O.substring(3).equals(downLine) && lineNo_D.substring(3).equals(upLine)) {
            CHANGE_STATION_SMALL = (int) ((Double.parseDouble(value.split(",")[2])) * 60);
        } else if (lineNo_O.substring(3).equals(downLine) && lineNo_D.substring(3).equals(downLine)) {
            CHANGE_STATION_SMALL = (int) ((Double.parseDouble(value.split(",")[3])) * 60);
        }
        return CHANGE_STATION_SMALL;
    }

    /**
     * 计算乘客到达换乘站的时刻
     *
     * @param lineNo_first  乘客在始发站乘车线路编码
     * @param string_O2     始发站
     * @param upStationTime         在始发站上车时间
     * @param changeStation 换乘站
     * @return timed_change_first 到达换乘站的时刻
     */
    private static Date timed_change(String lineNo_first, String string_O2, Date upStationTime, String changeStation, Date dayDate) throws ParseException {

        Integer lineNo_first_no = null;
        Date timed_change_first = null;
        List<BaseSubway> lineTimeTable = lbs.get(lineNo_first); //起点站所在路线的列车发车时刻表
        for (int subwayNo = 0; subwayNo < lineTimeTable.size() && timed_change_first == null; subwayNo++) {
            for (int stationNo = 0; stationNo < lineTimeTable.get(subwayNo).getStations().size(); stationNo++) {
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_O2)) {
                    lineNo_first_no = matchSubwayNo(lineNo_first, subwayNo, stationNo, upStationTime, dayDate);
                }
                if (lineNo_first_no != null) {
                    //若乘客刚好到达换乘站点
                    if (lineTimeTable.get(lineNo_first_no).getStations().get(stationNo).getStation().equals(changeStation)) {
                        timed_change_first = addDayDate(lineTimeTable.get(lineNo_first_no).getStations().get(stationNo).getArrivalDate(), dayDate);
                        break;
                    }
                }
            }
        }
        return timed_change_first;
    }

    /**
     * 反推乘客在换乘站出发时间
     *
     * @param lineNo_last   换乘站的路线
     * @param string_D2     终点站
     * @param downStationTime         到达终点站的时间
     * @param changeStation 换乘站
     * @return timeo_change_first 在换乘站出发的时间
     */
    private static Date timeo_change(String lineNo_last, String string_D2, Date downStationTime, String changeStation, Date dayDate) throws ParseException {

        Integer lineNo_last_no = null;
        Date timeo_change_first = null;
        List<BaseSubway> lineTimeTable = lbs.get(lineNo_last); //终点站所在路线的列车发车时刻表
        for (int subwayNo = 0; (subwayNo < lineTimeTable.size() && timeo_change_first == null) || subwayNo == lineTimeTable.size(); subwayNo++) {
            for (int stationNo = 0; stationNo < lineTimeTable.get(0).getStations().size(); stationNo++) {
                if (lineNo_last_no != null) {
                    //由乘客到达终点站乘坐的车次反推回到换乘站点
                    if (lineTimeTable.get(lineNo_last_no).getStations().get(stationNo).getStation().equals(changeStation)) {
                        timeo_change_first = addDayDate(lineTimeTable.get(lineNo_last_no).getStations().get(stationNo).getDate(), dayDate);
                        break;
                    }
                } else if (subwayNo < lineTimeTable.size() && lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_D2)) {
                    lineNo_last_no = matchSubwayNo(lineNo_last, subwayNo, stationNo, downStationTime, dayDate);
                }
            }
        }
        return timeo_change_first;
    }

    /**
     * 匹配乘客乘坐的车次号
     * @param lineNo 路线编码
     * @param subwayNo 当前车次号
     * @param stationNo 当前站点号
     * @param subwayTime 乘客上/下车时间
     * @param dayDate 列车运营日期
     * @return trueSubwayNo
     */
    private static Integer matchSubwayNo(String lineNo, Integer subwayNo, Integer stationNo, Date subwayTime, Date dayDate) {

        Integer trueSubwayNo = null;
        List<BaseSubway> lineTimeTable = lbs.get(lineNo);

        if (subwayNo == 0
                && (addDayDate(lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate(), dayDate).after(subwayTime)
                || (addDayDate(lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate(), dayDate).equals(subwayTime)))) { //首班车
            trueSubwayNo = 0;
        } else if (subwayNo != 0
                && addDayDate(lineTimeTable.get(subwayNo - 1).getStations().get(stationNo).getDate(), dayDate).before(subwayTime) //非首末班车
                && (addDayDate(lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate(), dayDate).after(subwayTime)
                || (addDayDate(lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate(), dayDate).equals(subwayTime)))) {
            if (matchThisSubway(subwayTime, addDayDate(lineTimeTable.get(subwayNo - 1).getStations().get(stationNo).getArrivalDate(), dayDate),
                    addDayDate(lineTimeTable.get(subwayNo).getStations().get(stationNo).getArrivalDate(), dayDate))) {
                trueSubwayNo = subwayNo;
            } else trueSubwayNo = subwayNo - 1;
        } else if (addDayDate(lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate(), dayDate).before(subwayTime)) { //末班车
            trueSubwayNo = lineTimeTable.size() - 1;
        }
        return trueSubwayNo;
    }

    /**
     * 时间上是否匹配当前车次
     * @param passengerDate 乘客在站点的上下车时间
     * @param formerSubwayArrivalDate 上一列车到站时间
     * @param thisSubwayArrivalDate 当前列车到站时间
     * @return isMatchThisSubway
     */
    private static Boolean matchThisSubway(Date passengerDate, Date formerSubwayArrivalDate, Date thisSubwayArrivalDate) {

        Boolean isMatchThisSubway = false;

        Long diffWithFormerSubway = TimeUtils.apply().date2Stamp(passengerDate) - TimeUtils.apply().date2Stamp(formerSubwayArrivalDate);
        Long diffWithThisSubway = TimeUtils.apply().date2Stamp(thisSubwayArrivalDate) - TimeUtils.apply().date2Stamp(passengerDate);
        Long minDiff = Math.min(diffWithFormerSubway, diffWithThisSubway);

        if (minDiff.equals(diffWithThisSubway)) {
            isMatchThisSubway = true;
        }
        return isMatchThisSubway;
    }

    /**
     * 找出所有OD路径中的时间最接近实际时间的一条路径，这里需要考虑实际乘车时间和路径产生的时间之差的阈值设定，本代码目前没有考虑最大阈值
     *
     * @param value 所有路径List
     */
    private static String CloseTimeLine(List<String> value, double realTime) {

        String[] timeList = value.get(0).split("#")[1].split(" ");
        Double closeTime = Double.parseDouble(timeList[timeList.length - 1]); //使用第一条路径所花费时间初始化closeTime
        String closeLine = value.get(0); //使用第一条路径初始化closeLine

        for (int m = 1; m < value.size(); m++) {
            String[] times = value.get(m).split("#")[1].split(" "); //时间列表
            if (Double.parseDouble(times[times.length - 1]) <= realTime && Double.parseDouble(times[times.length - 1]) >= closeTime) {
                closeTime = Double.parseDouble(times[times.length - 1]);
                closeLine = value.get(m);
            }
        }
        return closeLine;
    }

    /**
     * 给路径根据换乘次数分类汇总
     * @param routes OD输出的路径
     * @return 换乘次数分类汇总的结果列表
     */
    private static List<Integer> countChangeTimes(List<String> routes) {

        int changeOneTimeRoute = 0;
        int changeTwoTimeRoute = 0;
        int changeThreeTimeRoute = 0;
        int changeFourTimeRoute = 0;
        for (String route : routes) {
            String transferTime = route.split("#")[1].split(" ")[0];
            switch (transferTime) {
                case "1":
                    changeOneTimeRoute++;
                    break;
                case "2":
                    changeTwoTimeRoute++;
                    break;
                case "3":
                    changeThreeTimeRoute++;
                    break;
                case "4":
                    changeFourTimeRoute++;
                    break;
            }
        }
        return Arrays.asList(changeOneTimeRoute, changeTwoTimeRoute, changeThreeTimeRoute, changeFourTimeRoute);
    }

    /**
     * 给列车时刻表添加日期
     * @param hourDate 未添加日期的时刻表
     * @param dayDate 当天日期
     * @return 当天的时刻表
     */
    private static Date addDayDate(Date hourDate, Date dayDate) {

        Long stamp = TimeUtils.apply().date2Stamp(hourDate) + TimeUtils.apply().date2Stamp(dayDate);
        return TimeUtils.apply().stamp2Date(stamp);
    }
}