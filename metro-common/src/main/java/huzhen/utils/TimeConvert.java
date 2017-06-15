package huzhen.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hu on 2016/11/3.
 *
 *  时间 日期 格式转换函数
 */
public class TimeConvert {

    public static long HourToSeconds(String string) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("HH:mm:ss");
        Date date =sdf.parse(string);
//        long time =date.getTime()/1000;
        return  date.getTime()/1000;
    }
    public static Date String2Date(String string) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("HH:mm:ss");
        Date date =sdf.parse(string);
        return date;
    }
    public static String Date2String(Date date){
        SimpleDateFormat sdf =new SimpleDateFormat("HH:mm:ss");
        return sdf.format(date);
    }

    //����ת��Ϊʱ����
    public static String Second2Hour(long timelong){
        String timeStr =null;
        int hour =0;
        int minute =0;
        int second =0;
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
        String retStr =null;
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
