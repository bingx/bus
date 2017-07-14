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
public class ProvRoadHistoryData {
    private static FileReader fileReader;
    public static void main(String[] args) throws FileNotFoundException {
        DealDataBolt dealBolt = new DealDataBolt();
        String fileName = args[0].toString();
        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }
        fileReader = new FileReader(fileName);
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("/");
        GlobalInfo.writeFileName = fileNames[fileNames.length-1];
        GlobalInfo.outPath = Cfg.pathMap.get(Cfg.PROVROAD_OUT);;

        String str;
        String vehicletype = "coach", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "ProvRoad";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        try {
            // Read all lines
            while ((str = reader.readLine()) != null) {
                try{
                    //系统时间、城市编码，车牌号、颜色、GPS上报时间、经度、纬度、速度、方向、？、？、？
                    lineItem = str.split(",");
//                0->2016-01-01 00:00:00
//                1->A
//                2->粤AE4386
//                3->2
//                4->2015-12-31 23:59:52
//                5->22.991766666666667
//                6->113.27958333333333
//                7->40
//                8->146
//                9->7
//                10->1947911445
//                11->000000000000
//                12->000000000000000000
//                13->1
//                14->0320000000000000
                    //"vehicletype","vehicleid","x","y","devicetime","deviceid","speed","direction","extrainfo"
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    vehicleid = lineItem[2];
                    x = Float.parseFloat(lineItem[6]);
                    y = Float.parseFloat(lineItem[5]);
                    timestamp = 0;
                    try {
                        devicetime = lineItem[4];//2015-12-31 23:59:52
                        timestamp = df.parse(devicetime).getTime() / 1000;
                    } catch (ParseException e) {
                        //e.printStackTrace();
                        continue;
                    }
                    speed = Float.parseFloat(lineItem[7]);
                    direction = Float.parseFloat(lineItem[8]);


                    Record info = new Record(vehicletype, vehicleid, x, y,
                            timestamp, deviceid, speed, direction, extrainfo);
                    dealBolt.dealData(info);
                    str = null;
                }catch (Exception e){continue;}
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
