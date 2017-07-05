package cn.ODBackModel.subwayPkg;


import cn.ODBackModel.subwayPkg.pojo.BaseSubway;
import cn.ODBackModel.tableTable.TimeTableReader;
import cn.ODBackModel.tableTable.pojo.TimeTableStation;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by hu on 2016/11/3.
 * 用于发车
 * 每辆车拥有唯一标识：线路编码+车次编码
 */
public class BaseSubwayFactory {

    /**
     * 时间自增
     *
     * @param date 上一辆车的发车时刻
     * @param timeSpan 以秒为单位
     * @return date 当前车次的发车时刻
     */
    public Date addTime(Date date, Integer timeSpan) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.SECOND, timeSpan);
        return calendar.getTime();
    }

    /**
     * 用于生成一辆新的车辆信息，需要线路编号，上下行。
     * 测试
     *
     * @param subwayNO     车次编号
     * @param lineNo       线路编号
     * @param eachTimeSpan 较上一次发车时刻表的偏移量
     * @return subway 当前车次
     */
    public BaseSubway getSubway(Integer subwayNO, String lineNo, Integer eachTimeSpan) {
        BaseSubway subway;
        subway = new BaseSubway();

        String lineSubwayNO = lineNo + subwayNO;
        subway.setSubwayNO(lineSubwayNO);

        //路径list
        List<TimeTableStation> ls = new ArrayList<TimeTableStation>();

        //用于深拷贝的临时站点变量，不然会出现浅拷贝，所有车次的时间都一样
        TimeTableStation temp;
        for (TimeTableStation tts : TimeTableReader.getLineTimeTable(lineNo)) {
            String siteNo = tts.getStation();
            Date departureDate = addTime(tts.getDate(), eachTimeSpan);
            Date arrivalDate = addTime(tts.getArrivalDate(), eachTimeSpan);
            temp = new TimeTableStation(siteNo, departureDate, arrivalDate);
            ls.add(temp);
        }
        subway.setStations(ls);
        return subway;
    }

    public static void main(String[] args) {
        //获取当前地铁的乘客人数
        BaseSubway bs = new BaseSubwayFactory().getSubway(11, "2610", 1000);
        Double passengerNum = bs.getPassengerNum();
        System.out.println(passengerNum);
    }
}