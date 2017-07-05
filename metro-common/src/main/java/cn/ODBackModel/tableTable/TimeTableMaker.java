package cn.ODBackModel.tableTable;

import cn.ODBackModel.subwayPkg.BaseSubwayFactory;
import cn.ODBackModel.subwayPkg.pojo.BaseSubway;
import cn.ODBackModel.tableTable.pojo.TimeTableStation;
import cn.ODBackModel.utils.PropertiesUtil;
import cn.ODBackModel.utils.TimeConvert;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 添加列车在站点的候车时间，建立每一条线路的列车发车时刻表
 * Created by hu on 2016/11/3.
 */
public class TimeTableMaker {

    //运行截止时间
    private static String deadTime;

    //地铁工厂
    private static BaseSubwayFactory baseSubwayFactory;

    /**
     * 实例化对象，读取配置文件中的截至时间
     */
    public static TimeTableMaker ttm;
    static {
        ttm = new TimeTableMaker();
        ttm.ReadProperties();
    }

    public void ReadProperties(){
        try {
            PropertiesUtil propertiesUtil = new PropertiesUtil("runner.properties");
            deadTime = propertiesUtil.readValue("deadTime");
        } catch (IOException e) {
            System.out.println("配置文件读取错误!");
            e.printStackTrace();
        }
        baseSubwayFactory = new BaseSubwayFactory();
    }

    /**
     * 时间自增
     * @param date 当前列车发车时刻
     * @param timeSpan 发车间隔，以秒为单位
     * @return 下一列车发车时刻
     */
    public Date addTime(Date date, Integer timeSpan){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.SECOND, timeSpan);
        return calendar.getTime();
    }

    /**
     *  添加每一条路线列车各站点一天的发车时间
     */
    public Map<String,List<BaseSubway>> makeSubwayList() throws ParseException {

        Map<String,List<BaseSubway>> lbs =new HashMap<String,List<BaseSubway>>();
        //5号线
        lbs.put("2630", makeOneLineSubway("2630", 327));//这里加的时间在后面没有用上，只是早高峰发车时间间隔（初始化）
        lbs.put("2631", makeOneLineSubway("2631", 360));
        //3号线
        lbs.put("2610", makeOneLineSubway("2610", 409));
        lbs.put("2611", makeOneLineSubway("2611", 428));
        //4号线
        lbs.put("2620", makeOneLineSubway("2620", 186));
        lbs.put("2621", makeOneLineSubway("2621", 246));
        //1号线
        lbs.put("2680", makeOneLineSubway("2680", 180));
        lbs.put("2681", makeOneLineSubway("2681", 180));
        //2号线
        lbs.put("2600", makeOneLineSubway("2600", 360));
        lbs.put("2601", makeOneLineSubway("2601", 360));
        //11号线
        lbs.put("2410", makeOneLineSubway("2410", 600));
        lbs.put("2411", makeOneLineSubway("2411", 600));
        //9号线
        lbs.put("2670", makeOneLineSubway("2670", 600));
        lbs.put("2671", makeOneLineSubway("2671", 600));
        //7号线
        lbs.put("2650", makeOneLineSubway("2650", 600));
        lbs.put("2651", makeOneLineSubway("2651", 600));

        return lbs;
    }

    /**
     * 生成单条线的车次列表
     * @param lineName 线路编号
     * @param timeSpan 发车间隔
     * @return subwayList
     */
    private List<BaseSubway> makeOneLineSubway(String lineName,Integer timeSpan) throws ParseException {
        //发车时间
        Date start;
        //获取对应路线的初始发车时刻表
        List<TimeTableStation> lm = TimeTableReader.getLineTimeTable(lineName);
        //初始发车时间
        start = lm.get(0).getDate();

        //初始发车班次号
        Integer subwayNO = 0;

        //生成的车次list
        List<BaseSubway> subwayList = new ArrayList<BaseSubway>();

        //暂时记录当前时间偏移量，因为subwayNO从0开始，自乘的话会出现一直为0的BUG
        Integer eachTimeSpan=0;

        //在发车截止时间前的车都发出去
        while(checkDeadTime(start)){
            //计算每次发车的时间与时刻表的差
            subwayList.add(baseSubwayFactory.getSubway(subwayNO, lineName, eachTimeSpan));

            /**
             *根据早晚高峰 制作的列车时刻表
             */
            timeSpan =timeSpanForm(start,lineName,timeSpan);
            //时间偏移累加
            eachTimeSpan += timeSpan;

            //车次号自增
            subwayNO++;
            //发车时间自增
            start = addTime(start,timeSpan);
        }
        return subwayList;
    }

    /**
     * 检查列车目前是否已经运行到了截止时间,用于停止发车循环
     * @param date 当前列车到站时刻
     * @return Boolean
     */
    public boolean checkDeadTime(Date date){
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
        try {
            Date deadTimeDate = format.parse(deadTime);
            //比较列车是否在断面时间之前
            if(date.before(deadTimeDate)){
                return true;
            } else {
                return false;
            }
        }catch (Exception e){
            System.out.println("配置的断面时间有误!");
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 读取列车发车时刻间隔表timeIntervalForLine.csv
     * @return mapTimeInter 每一条线路对应的初始发车时刻
     */
    public Map<String,String> readTimeIntervalForLine(){

        Map<String,String> mapTimeInter = new HashMap<String, String>();

        String pathTimeInter ="timeIntervalForLine.csv";
        InputStream is =this.getClass().getClassLoader().getResourceAsStream(pathTimeInter);

        Scanner scanTimeInter =new Scanner(is,"UTF-8");

        scanTimeInter.nextLine();
        while(scanTimeInter.hasNext()){
            String line = scanTimeInter.nextLine();
            String[] strings =line.split(",");

            String key =strings[1]; //路线编码
            String value =strings[2]+","+strings[3]+","+strings[4]+","+strings[5]; //发车时刻间隔
            mapTimeInter.put(key, value);
        }

        return mapTimeInter;
    }

    /**
     * 根据不同早晚高峰时间段生成发车时间间隔
     * @param start 发车时间
     * @param lineName 路线编码
     * @param timeSpan 初始化发车时间间隔
     * @return timeSpan 实际发车时间间隔
     * @throws ParseException 抛出时间解析异常
     */
    public  Integer timeSpanForm(Date start,String lineName,int timeSpan) throws ParseException {

        Map<String,String> mapTimeInter =readTimeIntervalForLine();

        if (start.before(TimeConvert.String2Date("7:00:00"))){//平峰
            timeSpan = Integer.parseInt(mapTimeInter.get(lineName).split(",")[3]);
        }else if (start.before(TimeConvert.String2Date("9:30:00"))){//早高峰
            timeSpan = Integer.parseInt(mapTimeInter.get(lineName).split(",")[0]);
        }else if (start.before(TimeConvert.String2Date("16:30:00"))){//平峰
            timeSpan = Integer.parseInt(mapTimeInter.get(lineName).split(",")[3]);
        }else if (start.before(TimeConvert.String2Date("20:00:00"))){//晚高峰
            timeSpan = Integer.parseInt(mapTimeInter.get(lineName).split(",")[1]);
        }else if (start.before(TimeConvert.String2Date("21:30:00"))){//次高峰
            timeSpan = Integer.parseInt(mapTimeInter.get(lineName).split(",")[2]);
        }else if (start.before(TimeConvert.String2Date("23:00:00"))){//平峰
            timeSpan = Integer.parseInt(mapTimeInter.get(lineName).split(",")[3]);
        }
        return timeSpan;
    }

    public static Map<String, List<BaseSubway>> getSubwayList() {
        Map<String, List<BaseSubway>> subwayList = null;
        try {
        subwayList =  new TimeTableMaker().makeSubwayList();
    }catch (ParseException e) {
        e.printStackTrace();
    }
    return subwayList;
    }

    public static void main(String[] args) {
        /**
         * libs.size():16
         * 2680:228 --2681:228          1号线
         * 2600:139 --2601:139          2号线
         * 2610:132 --2611:131          3号线
         * 2620:212 --2621:223          4号线
         * 2630:142 --2631:139          5号线
         * 2410:99  --2411:99           11号线
         * 2650:99  --2651:99           7号线
         * 2670:99  --2671:99           9号线
         */
//        try {
            //Map<String, List<BaseSubway>> lbs = new TimeTableMaker().makeSubwayList();
            Map<String, List<BaseSubway>> lbs = getSubwayList();
            //地铁线路数（分方向）
            System.out.println("lbs.size():"+lbs.size());
            //每条线路的班次数
            System.out.println(lbs.get("2680").size() + "--" + lbs.get("2681").size());
            System.out.println(lbs.get("2600").size() + "--" + lbs.get("2601").size());
            System.out.println(lbs.get("2610").size() + "--" + lbs.get("2611").size());
            System.out.println(lbs.get("2620").size() + "--" + lbs.get("2621").size());
            System.out.println(lbs.get("2630").size() + "--" + lbs.get("2631").size());
            System.out.println(lbs.get("2410").size() + "--" + lbs.get("2411").size());
            System.out.println(lbs.get("2651").size() + "--" + lbs.get("2651").size());
            System.out.println(lbs.get("2671").size() + "--" + lbs.get("2671").size());

            System.out.println("Over!");
//        } catch (ParseException e) {
//        e.printStackTrace();
//    }
    }
}
