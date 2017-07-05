package cn.ODBackModel.tableTable.pojo;

import java.util.Date;

/**
 * Created by hu on 2016/11/3.
 *
 *  每个站点的列车到站时间和出发时间
 */
public class TimeTableStation {

    private Date date;

    private Date arrivalDate;

    private String station;

    public Date getDate() {
        return date;
    }

    public Date getArrivalDate() {
        return arrivalDate;
    }

    public String getStation() {
        return station;
    }

    public TimeTableStation(String str, Date da, Date arrival){
        this.station = str;
        this.date = da;
        this.arrivalDate = arrival;
    }
}
