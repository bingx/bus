package com.sibat.traffic;

import util.Cfg;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by User on 2017/5/22.
 */
public class Test {
    public static void main(String[] args) throws IOException, ParseException {
        String str ="/home/datum/storm/data/GPS/e6_data/STRING_20160103";
        String[] itesm=str.split("/");


        System.out.println(itesm[itesm.length-1]);
       // writeToFile("sdfsdfsfsd","hello");
        System.out.println(Cfg.pathMap.get(Cfg.BUS_DIRECTORY_PATH));
        String fileName = "E:\\infolog.log";
        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }
        System.out.println("exitst！");
        SimpleDateFormat sdf  =   new  SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        String timeStr = "2016-01-1 0:4:40";
       // Date date = new Date(timeStr);
        Date  d2=sdf.parse(timeStr);
        d2.getTime();
        System.out.println(sdf.format(d2)+"------------"+d2.getTime());

    }
}
