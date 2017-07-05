package cn.ODBackModel.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hu on 2016/11/3.
 *
 *  时间 日期 格式转换函数
 */
public class TimeConvert {

    //字符串格式转日期格式1
    public static Date String2Date(String string) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("HH:mm:ss");
        return sdf.parse(string);
    }

    //字符串格式转日期格式2
    public static Date String2DayDate(String string) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd");
        return sdf.parse(string);
    }

    //以秒为单位的时间戳转换为时间格式
    public static String Second2Hour(long timelong){
        String timeStr;
        int hour;
        int minute;
        int second;
        int time =(int)timelong;
        if(time<=0){
            return "00:00:00";
        }else {
            minute =time/60;
            if (minute<60){
                second =time%60;
                timeStr =unitFormat(0)+":"+unitFormat(minute) +":"+unitFormat(second);
            }else {
                hour =minute/60;
                if (hour>99)
                    return "99:59:59";
                minute =minute%60;
                second =time -hour*3600 -minute*60;
                timeStr =unitFormat(hour) +":"+unitFormat(minute)+":"+unitFormat(second);

            }
        }
        return timeStr;
    }
    public static String unitFormat(int i){
        String retStr;
        if(i>=0 && i<10){
            retStr ="0"+Integer.toString(i);
        }else
            retStr =""+i;
        return retStr;
    }

    /**
     *  测试函数
     * @param args 时间戳秒转换为Date格式
     */
    public static void main(String[] args) {
        String time =Second2Hour(30075);
        System.out.println(time);
    }
}
