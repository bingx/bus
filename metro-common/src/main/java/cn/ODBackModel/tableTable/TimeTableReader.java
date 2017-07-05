package cn.ODBackModel.tableTable;

import cn.ODBackModel.tableTable.pojo.TimeTableStation;
import cn.ODBackModel.utils.NoNameExUtil;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hu on 2016/11/3.
 *
 *  读取列车时刻表
 */
public class TimeTableReader {
    //路线时刻表
    private static Map<String,List<TimeTableStation>> lineTimeTable;

    //存放两个相邻站点属于哪条线,station+station: lineName
    private static Map<String,String> lineStation2station = new HashMap<>();
    //存放任意两个站点属于哪条线，因为有可能重合，此处只能用作判断任意两个站点是否属于同一条线
    private static Map<String,String> linesO2D = new HashMap<>();

    /**
     * 实例化对象，读取时刻表
     */
    public static TimeTableReader ttr;
    static {
        ttr = new TimeTableReader();
        try {
            ttr.ReadLineTimeTable();
            ReadLineByStation2Station();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取发车时刻，线路为数字，后面0(上行)1(下行)
     * Map（key: 路线， Value: 对应路线中站点编号和发车时刻组成的列表）
     * @return false 为啥是false？不应该是true?
     */
    private boolean ReadLineTimeTable() throws IOException{
        Map<String,List<TimeTableStation>> mlm = new HashMap<String, List<TimeTableStation>>();

        //1号线
        mlm.put("2680", readTimeTable("一号线上行(工作日).csv"));
        mlm.put("2681", readTimeTable("一号线下行(工作日).csv"));
        //2号线
        mlm.put("2600", readTimeTable("二号线上行(工作日).csv"));
        mlm.put("2601", readTimeTable("二号线下行(工作日).csv"));
        //3号线
        mlm.put("2610", readTimeTable("三号线上行(工作日).csv"));
        mlm.put("2611", readTimeTable("三号线下行(工作日).csv"));
        //4号线
        mlm.put("2620", readTimeTable("四号线上行(工作日).csv"));
        mlm.put("2621", readTimeTable("四号线下行(工作日).csv"));
        //5号线
        mlm.put("2630", readTimeTable("五号线上行(工作日).csv"));
        mlm.put("2631", readTimeTable("五号线下行(工作日).csv"));
        //11号线
        mlm.put("2410", readTimeTable("十一号线上行(工作日).csv"));
        mlm.put("2411", readTimeTable("十一号线下行(工作日).csv"));
        //7号线
        mlm.put("2650", readTimeTable("七号线上行(工作日).csv"));
        mlm.put("2651", readTimeTable("七号线下行(工作日).csv"));
        //9号线
        mlm.put("2670", readTimeTable("九号线上行(工作日).csv"));
        mlm.put("2671", readTimeTable("九号线下行(工作日).csv"));

        lineTimeTable =  mlm;
        return false;
    }

    /**
     * 读取时刻表
     * @return mSD 由站点编号和发车时刻组成的数组列表
     */
    private List<TimeTableStation> readTimeTable(String LineName){
        String path = LineName;
        List<TimeTableStation> mSD = new ArrayList<TimeTableStation>();
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(path);
            Scanner scan = new Scanner(is, "UTF-8");
            scan.nextLine();//第一行信息，为标题信息,应该先跳过
            while (scan.hasNext()) {
                String line = scan.nextLine();
                String[] lines = line.split(",");

                String siteNo = NoNameExUtil.Name2NO(lines[0]);

                Date departureDate = format.parse(lines[1]);
                //由发车时刻和停站时间推测到站时刻
                Integer dwellTime = Integer.parseInt("-"+lines[2]);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(departureDate);
                calendar.add(Calendar.SECOND, dwellTime);
                Date arrivalDate = calendar.getTime();

                mSD.add(new TimeTableStation(siteNo, departureDate, arrivalDate));//(站点编号, 发车时刻, 到站时刻)作为value返回给路线编码key
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return mSD;
    }

    /**
     * 将任意两个相邻的站点存放在lineStation2station中
     * 将任意两个不相同的站点存放在linesO2D中
     */
    private static void ReadLineByStation2Station(){
        String lineName;
        List<TimeTableStation> list;
        //不需要处理的小线
        LinkedList<String> lls = new LinkedList<String>(){
            {
                add("260a0");
                add("260a1");
                add("262a0");
            }
        };

        for(Map.Entry<String,List<TimeTableStation>> meslt:lineTimeTable.entrySet()){
            list = meslt.getValue(); //站点编码和发车时刻以及停站时间组成的数组列表
            lineName = meslt.getKey(); //路线编码
            if(lls.contains(lineName)){
                continue; //过滤小线
            }
            for(int i=0; i<list.size()-1; i++){
                lineStation2station.put(list.get(i).getStation()+list.get(i+1).getStation(),lineName); //将一条路线中的相邻两个站点编码和对应路线编码放在lineStation2station中
            }
            for(int m=0;m<list.size();m++){
                for(int n =0;n<list.size();n++){
                    if (m!=n){
                        linesO2D.put(list.get(m).getStation()+list.get(n).getStation(),lineName); //将一条线路中任意两个不相同的站点和对应路线编码放在linesO2D中
                    }
                }
            }
        }
    }

    public static Map<String,String> getLineStation2station(){
        return lineStation2station;
    }
    public static Map<String,String> getLinesO2D(){
        return linesO2D;
    }
    public static List<TimeTableStation> getLineTimeTable(String lineName){
        return lineTimeTable.get(lineName);
    }

    public static void main(String[] args){
        //测试输出12620190001267022000这个两个站点对应的路线编码
        System.out.println(TimeTableReader.getLineStation2station().get("12620190001267022000"));
        System.out.println(TimeTableReader.getLinesO2D().get("12620190001267022000"));
        //ReadLineByStation2Station();
    }
}
