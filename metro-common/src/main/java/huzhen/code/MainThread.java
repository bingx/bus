package huzhen.code;

import huzhen.code.readForm.MapAllPath;
import huzhen.subwayPkg.pojo.BaseSubway;
import huzhen.tableTable.TimeTableMaker;
import huzhen.tableTable.TimeTableReader;
import huzhen.utils.*;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import static huzhen.code.readForm.MapTimeWalk.getMapWalk;
import static huzhen.utils.RouteChangeStationUtil.Route2ChangeStation;

/**
 * Created by hu on 2016/11/3.
 * Update by wing on 2017/6/3
 *  OD反推
 */
public class MainThread {

    private static int IN_STATION; //进站加等待时间记150s
    private static int OUT_STATION; //出站时间90s

    private static Map<String, String> lineStation2station = null;
    private static Map<String, String> linesO2D = null;
    private static Map<String, List<BaseSubway>> lbs = null;
    private static Map<String, String> mapWalk = null;

    private static Integer count = 0;//计算路径成功匹配的记录

    public static void main(String[] args) throws IOException, ParseException {

        //为了计算运行程序所用时间
        Long start = System.currentTimeMillis();

        IN_STATION = 150;
        OUT_STATION = 90;

        //           -------------开始-------------
        lineStation2station = TimeTableReader.getLineStation2station(); //获取同一路线中相邻站点和所在路线组成的Map
        linesO2D = TimeTableReader.getLinesO2D(); //获取同一路线中任意两个不相同的站点和所在路线组成的Map
        lbs = new TimeTableMaker().makeSubwayList(); //地铁各线路班次发车表
        mapWalk = getMapWalk(); //转乘时间

        String path1 = "metro-common/src/main/resources/part-m-after";
        Scanner scan1 = new Scanner(new FileInputStream(new File(path1)), CodingDetector.checkTxtCode(path1));

        String path3 = "result_20170212_test"; //输出
        BufferedWriter bw = new BufferedWriter(new FileWriter(path3));

        while (scan1.hasNext()) {
            String line = scan1.nextLine();
            String[] strings = line.split(",");

            long time1 = TimeConvert.HourToSeconds(strings[1].substring(11, 19));//16:53:59
            long time2 = TimeConvert.HourToSeconds(strings[6].substring(11, 19));
            double realTime = (time2 - time1) / 60.0;
            String cardNo = strings[0];

            //将OD站点名称转化为序号
            String string_O2;
            String string_D2;
            if (FunctionsUtils.isContainsChinese(strings[3])) {

                //若站点为中文名称
                string_O2 = NoNameExUtil.Name2Ser(strings[3]);//"黄贝岭"-->string_O:1263037000-->56
                string_D2 = NoNameExUtil.Name2Ser(strings[8]);//"新秀"-->string_D:1260039000-->57
            } else {
                //若站点为编号
                string_O2 = NoNameExUtil.NO2Ser((strings[3]));//"黄贝岭"-->string_O:1263037000-->56
                string_D2 = NoNameExUtil.NO2Ser((strings[8]));//"新秀"-->string_D:1260039000-->57
            }


            // ==========================正常输出========================
            // 读第三张表allPathNew8.txt，输出路径结果
            String matchedLine = CheckAllPathNew(string_O2, string_D2, realTime, strings);

            if (matchedLine != null) {
                count ++;
                String result = functionPrintData(cardNo, matchedLine);
                System.out.println(result);
                bw.write(result);
                bw.newLine();
                bw.flush();
            } else System.out.println("matched error!");
        }
        System.out.println("count:" + count);
        System.out.println("time:" + (System.currentTimeMillis() - start) / 1000 / 60.0 + "min"); //程序运行时间
        bw.close();
    }

    /**
     * OD反推核心算法
     *
     * @param string_O2 起点站
     * @param string_D2 终点站
     * @param realTime  乘车时间
     * @param strings   OD记录
     * @return matchedLine 匹配的路径
     */
    private static String CheckAllPathNew(String string_O2, String string_D2, double realTime, String[] strings) throws ParseException {

        String matchedLine = null;
        Boolean flagIsChange = false;

        Integer oneChangeNo = 0; //一次换乘
        Integer twoChangeNo = 0;
        Integer threeChangeNo = 0;
        Integer fourChangeNo = 0;

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

            /**
             * 若OD在同一条线路上
             *      若有直达的路径
             *          满足直达条件：输出直达路径
             *          不满足直达条件：继续下一直达路径循环判断
             *      没有直达路径
             *          输出时间最接近的路线
             */
            String lineNo = linesO2D.get(NoNameExUtil.Ser2NO(string_O2) + NoNameExUtil.Ser2NO(string_D2)); //路线编码
            if (lineNo != null) { //如果OD共线路
                if (transferTime.equals("0")) { //若有直达路径
                    Boolean flagIsOneNo = ODPathBack_isOneNo(lineO, strings);
                    if (flagIsOneNo) { //若该直达路径满足直达条件
                        matchedLine = value.get(valueNo);
                        break;
                    }
                } else { //若没有直达路径
                    matchedLine = CloseTimeLine(value, realTime);
                    break;
                }
            } else {

                /**
                 * 若OD不在同一条线路上
                 *  O点在哪一条线上，D点在哪一条线上                     --------处理换乘代码
                 *  有i次换乘的路径 --遍历所有
                 *      有满足条件的换乘路径：输出满足i次换乘条件且时间最接近的那条路径
                 *      没有满足条件的换乘路径：继续循环判断直到最后一条
                 *                          当前路径为最后一条且没有满足添加的换乘路径：输出时间上最接近的路径
                 */
                String changeStations = Route2ChangeStation(thisLine.split("#")[0].replaceAll(" ", "-"), lineStation2station);
                Boolean flagChange = ODPathBackIsChange(stations, changeStations, transferTime, strings);

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
                    } else if(!flagIsChange && valueNo == value.size()-1){//若仍没有匹配的路径则输入时间上最接近的一条路径
                        System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
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
                    } else if(!flagIsChange && valueNo == value.size()-1){//若仍没有匹配的路径则输入时间上最接近的一条路径
                        System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
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
                    } else if(!flagIsChange && valueNo == value.size()-1){//若仍没有匹配的路径则输入时间上最接近的一条路径
                        System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
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
                    }  else if(!flagIsChange && valueNo == value.size()-1){//若仍没有匹配的路径则输入时间上最接近的一条路径
                        System.out.println("No Matched Line in the path! get the time nearby Line");
                        matchedLine = CloseTimeLine(value, realTime);
                        break;
                    }
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
    private static String functionPrintData(String cardNo, String strings) {

        String stringCon = ConvertOD(strings);
        stringCon = cardNo + "," + stringCon;
        return stringCon;
    }

    /**
     * 判断是否满足换乘条件
     *
     * @param stations       站点列表
     * @param changeStations 换乘站点
     * @param transferTime   换乘次数
     * @param strings        OD记录数组
     * @return flag 是否满足换乘条件
     */
    private static boolean ODPathBackIsChange(String[] stations, String changeStations, String transferTime, String[] strings) throws ParseException {

        boolean flag = false;

        String date1 = strings[1].substring(11, 19);//O站进站时间
        String date2 = strings[6].substring(11, 19);//D站出站时间
        date1 = TimeConvert.Second2Hour(TimeConvert.HourToSeconds(date1) + 8 * 60 * 60 + IN_STATION);//刷卡进站时刻 + 等待时间150秒 = 上车时刻
        date2 = TimeConvert.Second2Hour(TimeConvert.HourToSeconds(date2) + 8 * 60 * 60 - OUT_STATION);//出站刷卡时刻 - 出站时间90秒  = 下车时刻

        String[] odChangeStations = changeStations.split("-");
        String string_O2 = odChangeStations[0]; //起点站序号
        String string_D2 = odChangeStations[1]; //终点站序号

        String station2stationFirst = NoNameExUtil.Ser2NO(stations[0]) + NoNameExUtil.Ser2NO(stations[1]);
        String lineNoFirst = lineStation2station.get(station2stationFirst); //上车线路
        String station2stationLast = NoNameExUtil.Ser2NO(stations[stations.length - 2]) + NoNameExUtil.Ser2NO(stations[stations.length - 1]);
        String lineNoLast = lineStation2station.get(station2stationLast); //下车线路

        switch (transferTime) {
            case "1": {// #1
                String changeStation = odChangeStations[2]; //换乘站编码

                //到达换乘站的时刻
                Date timed_change = timed_change(lineNoFirst, string_O2, TimeConvert.String2Date(date1), changeStation);
                //从换乘站出发的时刻
                Date timeo_change = timeo_change(lineNoLast, string_D2, TimeConvert.String2Date(date2), changeStation);
                //该站点最小换乘时间
                int CHANGE_STATION_SMALL = change_station_walkTime(changeStation, lineNoFirst, lineNoLast);

                if (TimeConvert.HourToSeconds(TimeConvert.Date2String(timeo_change)) - TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change)) >= CHANGE_STATION_SMALL) {
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

                Date timed_change_first = timed_change(lineNoFirst, string_O2, TimeConvert.String2Date(date1), changeStation1);
                int CHANGE_STATION_SMALL1 = change_station_walkTime(changeStation1, lineNoFirst, lineNo_Second);
                //由最短换乘时间计算乘客在第一个换乘站的出发时间
                Date timeo_change_first = TimeConvert.String2Date(TimeConvert.Second2Hour(TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_first)) + CHANGE_STATION_SMALL1));

                //由第一个换乘站出发的时间---timeo_change_first找到第二个换乘站到达的时间---timed_change_second
                Date timed_change_last = timed_change(lineNo_Second, changeStation1, timeo_change_first, changeStation2);
                int CHANGE_STATION_SMALL_LAST = change_station_walkTime(changeStation2, lineNo_Second, lineNoLast);

                //由到达终点站的时间反推最后一个换乘点出发的时间---timeo_change_last
                Date timeo_change_last = timeo_change(lineNoLast, string_D2, TimeConvert.String2Date(date2), changeStation2);

                if (TimeConvert.HourToSeconds(TimeConvert.Date2String(timeo_change_last)) - TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_last)) >= CHANGE_STATION_SMALL_LAST) {
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

                Date timed_change_first = timed_change(lineNoFirst, string_O2, TimeConvert.String2Date(date1), changeStation1);
                int CHANGE_STATION_SMALL1 = change_station_walkTime(changeStation1, lineNoFirst, lineNo_Second);
                Date timeo_change_first = TimeConvert.String2Date(TimeConvert.Second2Hour(TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_first)) + CHANGE_STATION_SMALL1));

                Date timed_change_second = timed_change(lineNo_Second, changeStation1, timeo_change_first, changeStation2);
                int CHANGE_STATION_SMALL2 = change_station_walkTime(changeStation2, lineNo_Second, lineNo_Third);
                Date timeo_change_second = TimeConvert.String2Date(TimeConvert.Second2Hour(TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_second)) + CHANGE_STATION_SMALL2));

                Date timed_change_last = timed_change(lineNo_Third, changeStation2, timeo_change_second, changeStation3);
                int CHANGE_STATION_SMALL_LAST = change_station_walkTime(changeStation3, lineNo_Third, lineNoLast);
                Date timeo_change_last = timeo_change(lineNoLast, string_D2, TimeConvert.String2Date(date2), changeStation3);

                if (TimeConvert.HourToSeconds(TimeConvert.Date2String(timeo_change_last)) - TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_last)) >= CHANGE_STATION_SMALL_LAST) {
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

                Date timed_change_first = timed_change(lineNoFirst, string_O2, TimeConvert.String2Date(date1), changeStation1);
                int CHANGE_STATION_SMALL1 = change_station_walkTime(changeStation1, lineNoFirst, lineNo_Second);
                Date timeo_change_first = TimeConvert.String2Date(TimeConvert.Second2Hour(TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_first)) + CHANGE_STATION_SMALL1));


                Date timed_change_second = timed_change(lineNo_Second, changeStation1, timeo_change_first, changeStation2);
                int CHANGE_STATION_SMALL2 = change_station_walkTime(changeStation2, lineNo_Second, lineNo_Third);
                Date timeo_change_second = TimeConvert.String2Date(TimeConvert.Second2Hour(TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_second)) + CHANGE_STATION_SMALL2));

                Date timed_change_third = timed_change(lineNo_Third, changeStation2, timeo_change_second, changeStation3);
                int CHANGE_STATION_SMALL3 = change_station_walkTime(changeStation3, lineNo_Third, lineNo_Forth);
                Date timeo_change_third = TimeConvert.String2Date(TimeConvert.Second2Hour(TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_third)) + CHANGE_STATION_SMALL3));

                Date timed_change_last = timed_change(lineNo_Forth, changeStation3, timeo_change_third, changeStation4);
                int CHANGE_STATION_SMALL_LAST = change_station_walkTime(changeStation4, lineNo_Forth, lineNoLast);
                Date timeo_change_last = timeo_change(lineNoLast, string_D2, TimeConvert.String2Date(date2), changeStation4);

                if (TimeConvert.HourToSeconds(TimeConvert.Date2String(timeo_change_last)) - TimeConvert.HourToSeconds(TimeConvert.Date2String(timed_change_last)) >= CHANGE_STATION_SMALL_LAST) {
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
    private static Boolean ODPathBack_isOneNo(String lineO, String[] strings) throws ParseException {

        Boolean flag = false;
        String date1 = strings[1].substring(11, 19); //O站出发时间2015-09-16T12:09:47.000Z-->08:21:15
        String date2 = strings[6].substring(11, 19); //D站到达时间2015-09-16T12:28:05.000Z-->09:09:30

        date1 = TimeConvert.Second2Hour(TimeConvert.HourToSeconds(date1) + 8 * 60 * 60 + IN_STATION); //刷卡进站+等待时间150秒
        date2 = TimeConvert.Second2Hour(TimeConvert.HourToSeconds(date2) + 8 * 60 * 60 - OUT_STATION); //出站刷卡时间90秒
        String string_O = NoNameExUtil.Name2NO(strings[3]); //O站点编码
        String string_D = NoNameExUtil.Name2NO(strings[8]); //D站点编码
        int subwayNo_O = 0;
        int subwayNo_D = 0;

        List<BaseSubway> lineTimeTable = lbs.get(lineO); //当前线路上的列车运行时刻表
        for (int subwayNo = 0; subwayNo < lineTimeTable.size(); subwayNo++) {
            for (int stationNo = 0; stationNo < lineTimeTable.get(subwayNo).getStations().size(); stationNo++) {
                //车辆运行到的当前车站为乘客起点站
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_O)) {
                    if (subwayNo == 0 && lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(TimeConvert.String2Date(date1))) {
                        subwayNo_O = 0;
                    } else if (subwayNo != 0 &&
                            lineTimeTable.get(subwayNo-1).getStations().get(stationNo).getDate().before(TimeConvert.String2Date(date1)) &&
                            lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(TimeConvert.String2Date(date1))) {
                        subwayNo_O = subwayNo;//乘客在O站乘坐的列车为当前列车
                    }
                }
                //车辆运行到的当前车站为乘客终点站
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals((string_D))) {
                    if (subwayNo == 0 && lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(TimeConvert.String2Date(date2))) {
                        subwayNo_D = 0;
                    } else if (subwayNo != 0 &&
                            lineTimeTable.get(subwayNo-1).getStations().get(stationNo).getDate().before(TimeConvert.String2Date(date2)) &&
                            lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(TimeConvert.String2Date(date2))) {
                        subwayNo_D = subwayNo;//乘客在D站乘坐的列车为当前列车
                    }
                }
            }
        }
        if (subwayNo_O == subwayNo_D) {
            flag = true;
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
     * @param date1         在始发站上车时间
     * @param changeStation 换乘站
     * @return timed_change_first 到达换乘站的时刻
     */
    private static Date timed_change(String lineNo_first, String string_O2, Date date1, String changeStation) throws ParseException {

        Integer lineNo_first_no = null; //初始化在O站乘坐的列车号
        Date timed_change_first = null;
        List<BaseSubway> lineTimeTable = lbs.get(lineNo_first); //起点站所在路线的列车发车时刻表
        for (int subwayNo = 0; subwayNo < lineTimeTable.size(); subwayNo++) {
            for (int stationNo = 0; stationNo < lineTimeTable.get(subwayNo).getStations().size(); stationNo++) {
                //若当前车站为始发站
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_O2)) {
                    if (subwayNo == 0 && lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(date1)) {
                        lineNo_first_no = 0;
                    } else if (subwayNo != 0
                            && lineTimeTable.get(subwayNo-1).getStations().get(stationNo).getDate().before(date1)
                            && lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(date1)) {
                        lineNo_first_no = subwayNo;
                    }
                }
                //若列车匹配成功，乘客乘上列车
                if (lineNo_first_no != null) {
                    //若乘客刚好到达换乘站点
                    if (lineTimeTable.get(lineNo_first_no).getStations().get(stationNo).getStation().equals(changeStation)) {
                        timed_change_first = lineTimeTable.get(lineNo_first_no).getStations().get(stationNo).getArrivalDate();
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
     * @param date2         到达终点站的时间
     * @param changeStation 换乘站
     * @return timeo_change_first 在换乘站出发的时间
     */
    private static Date timeo_change(String lineNo_last, String string_D2, Date date2, String changeStation) throws ParseException {

        Integer lineNo_first_no = null;
        Date timeo_change_first = null;
        List<BaseSubway> lineTimeTable = lbs.get(lineNo_last); //终点站所在路线的列车发车时刻表
        for (int subwayNo = 0; subwayNo < lineTimeTable.size(); subwayNo++) {
            for (int stationNo = 0; stationNo < lineTimeTable.get(subwayNo).getStations().size(); stationNo++) {
                //若当前车站为终点站
                if (lineTimeTable.get(subwayNo).getStations().get(stationNo).getStation().equals(string_D2)) {
                    if (subwayNo == 0 && lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(date2)) {
                        lineNo_first_no = 0;
                    } else if (subwayNo != 0
                            && lineTimeTable.get(subwayNo - 1).getStations().get(stationNo).getDate().before(date2)
                            && lineTimeTable.get(subwayNo).getStations().get(stationNo).getDate().after(date2)) {
                        lineNo_first_no = subwayNo;
                    }
                }
                if (lineNo_first_no != null) {
                    //若乘客刚好到达换乘站点
                    if (lineTimeTable.get(lineNo_first_no).getStations().get(stationNo).getStation().equals(changeStation)) {
                        timeo_change_first = lineTimeTable.get(lineNo_first_no).getStations().get(stationNo).getDate();
                        break;
                    }
                }
            }
        }
        return timeo_change_first;
    }

    /**
     * 转化为最后的结果，每个站点以字符串表示
     */
    private static String ConvertOD(String string) {

        String[] strings = string.split("#")[0].split(" ");
        StringBuilder sb = new StringBuilder();
        for (String i : strings) {
            sb.append(NoNameExUtil.Ser2NO(i));
            sb.append("-");
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
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
}