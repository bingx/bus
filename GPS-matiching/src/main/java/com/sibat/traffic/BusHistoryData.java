package com.sibat.traffic;

import util.Cfg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.SimpleDateFormat;

/**
 * Created by User on 2017/5/23.
 */
public class BusHistoryData {
    public static void main(String[] args) throws FileNotFoundException {
        DealDataBolt dealBolt = new DealDataBolt();
        String fileName = args[0].toString();

        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }

        FileReader fileReader = new FileReader(fileName);

        String str;
        // Open the reader
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("/");
        GlobalInfo.writeFileName= fileNames[fileNames.length-1];
        GlobalInfo.outPath = Cfg.pathMap.get(Cfg.BUS_OUT);

        // Read all lines
        String vehicletype = "bus", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "bus";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        try {
            // Read all lines
            while ((str = reader.readLine()) != null) {
                try {
                    //系统时间、类型、终端号、车牌号、线路id、子线路id、公司、状态、经度、纬度、高度、GPS上报时间、定位速度、方向、行车记录仪速度、里程
                    lineItem = str.split(",");
                    devicetime = "20" + lineItem[11];//补齐为完整的日期格式
                    String[] date_time = devicetime.split(" ");
                    String time = date_time[1];
                    String[] h_m_s = time.split(":");
                    String h = h_m_s[0];
                    int hour = Integer.parseInt(h);
                    if (23 < hour || hour < 6) // 特殊时间，摒弃处理
                        continue;
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    vehicleid = lineItem[3];
                    x = Float.parseFloat(lineItem[8]);
                    y = Float.parseFloat(lineItem[9]);
                    speed = Float.parseFloat(lineItem[12]);
                    direction = Float.parseFloat(lineItem[13]);
                    timestamp = df.parse(devicetime).getTime() / 1000;
                    Record info = new Record(vehicletype, vehicleid, x, y,
                            timestamp, deviceid, speed, direction, extrainfo);
                    dealBolt.dealData(info);
                } catch (Exception e) {
                    continue;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
