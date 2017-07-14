package com.sibat.traffic;

import util.Cfg;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by User on 2017/5/19.
 */
public class E6HistioryData {
    private static FileReader fileReader;
    public static String writefileName = "";


    public static void main(String[] args) throws IOException {
        DealDataBolt dealBolt = new DealDataBolt();
        //  /home/datum/storm/data/GPS/e6_data/STRING_20160103
        String fileName = args[0].toString();
        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }

        String str = "";
        String vehicletype = "truck", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "E6";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        BufferedReader reader = null;
        try {
            fileReader = new FileReader(fileName);
            reader = new BufferedReader(fileReader);
            String fileNames[] = fileName.split("/");
            GlobalInfo.writeFileName =  fileNames[fileNames.length-1];
            System.out.println("out file name =>"+GlobalInfo.writeFileName);
            GlobalInfo.outPath =  Cfg.pathMap.get(Cfg.E6_OUT);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while ((str = reader.readLine()) != null) {
           // str = reader.readLine();
            try {
                lineItem = str.split(",");
                if (lineItem.length != 9)
                    continue;
                //"vehicletype","vehicleid","x","y","devicetime","deviceid","speed","direction","extrainfo"
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                vehicleid = lineItem[0];
                if (lineItem[1].toString().equals("\\") || lineItem[2].toString().equals("\\"))//存在一些非法的string 在转化之前进行过滤操作
                {
                    lineItem[1] = "0.000";
                    lineItem[1] = "0.000";
                }
                x = Float.parseFloat(lineItem[1]) / 1000000;
                y = Float.parseFloat(lineItem[2]) / 1000000;
                devicetime = "20" + lineItem[7];//补齐为完整的日期格式
                speed = Float.parseFloat(lineItem[4]);
                direction = Float.parseFloat(lineItem[5]);
                try {
                    timestamp = df.parse(devicetime).getTime() / 1000;//毫秒
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Record info = new Record(vehicletype, vehicleid, x, y,
                        timestamp, deviceid, speed, direction, extrainfo);
                dealBolt.dealData(info);
                str = null;
            } catch (NumberFormatException e) {
                System.out.println("[ERROR]NumberFormatException");
                continue;
            }

        }


    }


}
