package cn.ODBackModel.mainPkg.readForm;

import cn.ODBackModel.utils.NoNameExUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Created by hu on 2016/11/3.
 *
 * 读取   换乘走行时间-工作日-平峰.csv 表格   此表是直接拿过来用的
 *
 *  知道所有的换乘时间
 */
public class MapTimeWalk {

    static Map<String, String> mapWalk = new HashMap<String, String>();


    /**
     * 读取换乘走行时间文件
     */
    public static MapTimeWalk mtw;
    static {
        mtw =new MapTimeWalk();
        try {
            mtw.ReadTimeWalkFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将各转乘站点在不同路线间转乘的换乘时间组成Map
     * @throws IOException
     */
    public void ReadTimeWalkFile() throws IOException{
        String pathWalk = "换乘走行时间-工作日-平峰.csv";
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(pathWalk);

        Scanner scanWalk = new Scanner(is,"UTF-8");

        scanWalk.nextLine();
        if (scanWalk != null){
            while (scanWalk.hasNext()) {
                String line = scanWalk.nextLine();
                String[] strings = line.split(",");
                //将路线名称转换为路线编码
                for (int i = 1; i <= 2; i++) {
                    switch (strings[i]) {
                        case "1号线":
                            strings[i] = "268";
                            break;
                        case "2号线":
                            strings[i] = "260";
                            break;
                        case "3号线":
                            strings[i] = "261";
                            break;
                        case "4号线":
                            strings[i] = "262";
                            break;
                        case "5号线":
                            strings[i] = "263";
                            break;
                        case "11号线":
                            strings[i] = "241";
                            break;
                        case "7号线":
                            strings[i] = "265";
                            break;
                        case "9号线":
                            strings[i] = "267";
                            break;
                    }
                }
                //站点代码,原始路线代码,转乘路线代码
                String key = NoNameExUtil.Name2NO(strings[0]) + "," + strings[1] + "," + strings[2];
                //换乘时间
                String value = strings[3] + "," + strings[4] + "," + strings[5] + "," + strings[6];
                mapWalk.put(key, value);
            }
        }
    }

    /**
     * 返回换乘站点的换乘时间
     * @return mapWalk
     */
    public static Map<String,String> getMapWalk(){
        return mapWalk;
    }

    public static void main(String[] args) {
        //测试
        Map<String,String> mapWalk =getMapWalk();
        System.out.println(mapWalk.get("1268028000,268,241"));
        Set<String> set =mapWalk.keySet();
        for(String key:set){
            System.out.println(key+"-"+mapWalk.get(key));
        }
        System.out.println(mapWalk.size());
    }
}
