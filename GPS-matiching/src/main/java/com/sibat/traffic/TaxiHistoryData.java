package com.sibat.traffic;

import util.Cfg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by User on 2017/5/23.
 */
public class TaxiHistoryData {
    private static FileReader fileReader;
    public static String writefileName = "";

    public static void main(String[] args) throws FileNotFoundException {
        DealDataBolt dealBolt = new DealDataBolt();
        String str;
        // Open the reader
        String fileName = args[0].toString();
        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }
        fileReader = new FileReader(fileName);
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("/");
        GlobalInfo.writeFileName =  fileNames[fileNames.length-1];
        GlobalInfo.outPath = Cfg.pathMap.get(Cfg.TAXI_OUT);

        String vehicletype = "taxi", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "taxi";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        try {
            // Read all lines
            while ((str = reader.readLine()) != null) {
                try{
                    //车牌号、经度、纬度、上报时间、设备号、速度、方向、定位状态、报警类型、SIM卡号、载客状态、车牌颜色
                    lineItem = str.split(",");
//                0->粤B0V3P6
//                1->114.122849
//                2->22.579933
//                3->2016-01-01 00:05:12
//                4->1568616
//                5->9
//                6->180
//                7->0
//                8->
//                9->
//                10->0
//                11->蓝色
                    //"vehicletype","vehicleid","x","y","devicetime","deviceid","speed","direction","extrainfo"
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    vehicleid = lineItem[0];
                    x = Float.parseFloat(lineItem[1]);
                    y = Float.parseFloat(lineItem[2]);

                    timestamp = 0;
                    try {
                        devicetime = lineItem[3];//2016-01-01 00:05:12
                        timestamp = df.parse(devicetime).getTime() / 1000;
                    } catch (ParseException e) {
                        continue;
                    }
                    deviceid = lineItem[4];
                    speed = Float.parseFloat(lineItem[5]);
                    direction = Float.parseFloat(lineItem[6]);
                    Record info = new Record(vehicletype, vehicleid, x, y,
                            timestamp, deviceid, speed, direction, extrainfo);
                    dealBolt.dealData(info);

                }catch (Exception e){continue;}
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
