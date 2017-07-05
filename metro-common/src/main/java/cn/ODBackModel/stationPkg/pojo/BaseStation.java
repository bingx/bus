package cn.ODBackModel.stationPkg.pojo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by hu on 2016/11/3.
 * 站点模型，记录当前站点乘客数，以及在此换乘的乘客群List
 * 注：新进站的乘客也要添加到cntNum中
 */
public class BaseStation {

    //记录当前站点的人数
    private Double cntNum;


    //站点编号
    private String stationNO;

    //站点名称
    private String stationName;

    //记录上次到站的车次的时间，注意换线需要重置
    private Date arriveRecord;

    //包含此站点进站的实时乘客的时间刻度
    private List<String> realTimePassengers;

    /**
     * 换线和初始化的时候需要重置时间记录
     * 实时的不再需要重置
     */
    public void resetArriveRecord(){
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
        try {
            this.arriveRecord = format.parse("6:00:00");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化cntNum为0，transfPassengers为空
     * @param stationNO 站点编码
     * @param stationName 站点名称
     */
    public BaseStation(String stationNO,String stationName){
        this.stationName = stationName;
        this.stationNO = stationNO;
        this.cntNum = Double.valueOf(0);
        //初始化需要重置时间记录
        resetArriveRecord();
        this.realTimePassengers = new ArrayList<String>();
    }

    /**
     * 添加实时数据的乘客数，为list<String>，仅包含时间刻度
     * @param o_time
     */
    public void addToRealTimePassengers(String o_time){
        this.realTimePassengers.add(o_time);
    }

    public List<String> getRealTimePassengers() {
        return realTimePassengers;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public void setStationNO(String stationNO) {
        this.stationNO = stationNO;
    }

    public String getStationName() {
        return stationName;
    }

    public String getStationNO() {
        return stationNO;
    }

    public void setCntNum(Double cntNum) {
        this.cntNum = cntNum;
    }

    public Double getCntNum() {
        return cntNum;
    }


}
