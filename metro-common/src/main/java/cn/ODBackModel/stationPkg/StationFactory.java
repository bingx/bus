package cn.ODBackModel.stationPkg;

import cn.ODBackModel.stationPkg.pojo.BaseStation;
import cn.ODBackModel.utils.NoNameExUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hu on 2016/11/3.
 * 使用单例模式
 */
public class StationFactory {

    //该工厂的单例
    private static StationFactory stationFactory;

    //所有站点的map
    public static Map<String,BaseStation> allStations = new HashMap<String, BaseStation>();

    public StationFactory(){
        for(Map.Entry<String,String> entry : NoNameExUtil.No_Name.entrySet()){
            allStations.put(entry.getKey(),new BaseStation(entry.getKey(),entry.getValue()));
        }
    }

    /**
     * 惰性加载该工厂模式单例
     * @return
     */
    public static StationFactory getInstance(){
        if(stationFactory == null){
            stationFactory = new StationFactory();
        }
        return stationFactory;
    }

    /**
     * 从单例工厂中获取单例站点模型
     * @param stationNO
     * @return
     */
    public static BaseStation getStationInstance(String stationNO){
        return allStations.get(stationNO);
    }

    /**
     * 获取所有站点
     * @return
     */
    public static Map<String,BaseStation> getAllStationInstance(){
        return allStations;
    }

    public static void main(String[] args) {
        System.out.println(StationFactory.getInstance().allStations);
    }
}
