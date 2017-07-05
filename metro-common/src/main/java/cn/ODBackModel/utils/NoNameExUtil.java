package cn.ODBackModel.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by hu on 2016/11/3.
 *
 *  读取 Ser2No2Name      此表是从项目UtilsProject的subway.filterParser.java中 根据 站点编号表.csv 导出来的
 *      使得 No Ser Name 之间可以相互转换
 */
public class NoNameExUtil {

    public static HashMap<String,String> Name_No= new HashMap<String,String>(); //站点名称-站点编号
    public static HashMap<String,String> No_Name= new HashMap<String,String>();
    public static HashMap<String,String> Name_Ser= new HashMap<String,String>(); //站点名称-站点序号
    public static HashMap<String,String> Ser_Name= new HashMap<String,String>();
    public static HashMap<String,String> No_Ser= new HashMap<String,String>(); //站点编号-站点序号
    public static HashMap<String,String> Ser_No= new HashMap<String,String>();


    public static NoNameExUtil nne;

    static {
        nne =new NoNameExUtil();
        try {
            nne.ReadSerNoName();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ReadSerNoName() throws IOException{
        String path= "Ser2No2Name";
//        String path= "Ser2No2Name20161227";

        InputStream is =this.getClass().getClassLoader().getResourceAsStream(path);
        Scanner scan=new Scanner(is,"UTF-8");

        scan.nextLine();
        while(scan.hasNext())
        {
            //1268036000,机场东,268,1号线,1
            String line=scan.nextLine();
            String []splits=line.split(",");


            //站点编号-站点名称
            No_Name.put(splits[0], splits[1]);
            Name_No.put(splits[1], splits[0]);

            //站点序号-站点名称
            Ser_Name.put(splits[4], splits[1]);
            Name_Ser.put(splits[1], splits[4]);

            //站点编号-站点序号
            No_Ser.put(splits[0], splits[4]);
            Ser_No.put(splits[4], splits[0]);
        }
        scan.close();

    }

    public static String Name2NO(String name){
        return Name_No.get(name);
    }
    public static String NO2Name(String No){
        return No_Name.get(No);
    }

    public static String Name2Ser(String name){
        return Name_Ser.get(name);
    }
    public static String Ser2Name(String Ser){
        return Ser_Name.get(Ser);
    }

    public static String Ser2NO(String Ser){
        return Ser_No.get(Ser);
    }
    public static String NO2Ser(String No){
//        System.out.println("-world,No2Ser");
        return No_Ser.get(No);
    }

    public static void main(String []args)
    {
        for(String key: NoNameExUtil.Name_No.keySet())
        {
//            System.out.println(key+":"+ NoNameExUtil.Name_No.get(key));
        }
        System.out.println(No_Name.size());
        System.out.println(Name2NO("红山"));
        System.out.println(NO2Name("1262015000"));
        System.out.println(Name2Ser("机场东"));
        System.out.println(Ser2Name("50"));
        System.out.println(Ser2NO("50"));
        System.out.println(Ser2NO("60"));
        System.out.println(NO2Ser("1262015000"));
    }
}
