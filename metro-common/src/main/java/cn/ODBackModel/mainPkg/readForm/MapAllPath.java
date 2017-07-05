package cn.ODBackModel.mainPkg.readForm;

import cn.ODBackModel.mainPkg.pojo.AllPathRoute;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by hu on 2016/11/3.
 *
 * 读取allPathNew8.txt     此表是UtilsProject中做的 1 将allpath.txt 用记事本打开复制粘贴 可得到 alpathNew.txt 然后由代码排序换乘次数
 * 所有OD间的可能路径
 * 包括读取到最后一条路径Line  后面可以借鉴 *************
 *
 *
 * 2017-01-01
 * allPathNew8.txt 是自己生成的路径  SubwayAllPath项目生成的结果
 */
public class MapAllPath {
    /**
     * 新线开通后  新线开通后的路径 1，2，3，4，5，7，9，11
     */
    private static List<String> list = new ArrayList<String>(); //路径列表
    private static Map<String,List<String>> mapAllPath = new HashMap<String,List<String>>(); //OD: list
    private static AllPathRoute allPath;
    public static Map<String,List<String>> getAllPath(){
        return mapAllPath;
    }

    /**
     * 读取任意站点OD之间的路线并将路线添加到allPath和list中
     * @throws IOException
     */
    public void ReadAllPath() throws IOException{

        String path = "allPathNew8.txt";//新线开通后1、2、3、4、5、7、9、11 不包括 福邻
//        String path = "allpathNew8p.txt";//新线开通后1、2、3、4、5、7、9、11 包括 福邻

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(path);
        Scanner scan = new Scanner(is,"UTF-8");
        //1 1 #0 T 0.00000000  0.00000000
        String line = scan.nextLine();

        String[] strings = line.split("#");
        String[] strings1 = strings[0].split(" ");
        String o2d = strings1[0] + "-" + strings1[strings1.length-1]; //1-1

        allPath = new AllPathRoute(line,o2d);
        list.add(line);
        //开始读取每一行
        while (scan.hasNext()){
            line = scan.nextLine();
            strings = line.split("#");
            strings1 = strings[0].split(" ");
            o2d = strings1[0] + "-" + strings1[strings1.length-1]; //O-D

            if (list == null){
                //如果路线列表为空，则初始化列表为第一种路径
                list = new ArrayList<String>();
                list.add(allPath.getString());
            }
            if (allPath.getO2d().equals(o2d)){ //如果OD相同，将路线添加到list
                list.add(line);
            }else {
                mapAllPath.put(allPath.getO2d(),list); //O-D: list
                list = null; //将列表清空
                allPath.setString(line); //将当前读取的内容作为第一种路径添加到allPath中
                allPath.setO2d(o2d); //添加O-D
            }
        }

        //最后一条数据
        if (list != null){
            mapAllPath.put(allPath.getO2d(),list);
        }else {
            list =new ArrayList<>();
            list.add(allPath.getString());
            mapAllPath.put(allPath.getO2d(),list);
        }
    }

    /**
     * 读取文件，将所有路径list按不同OD加入到Map
     */
    public static MapAllPath mmm;
    static {
        mmm =new MapAllPath();
        try {
            mmm.ReadAllPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //得到所有OD拥有的路径列表组成的Map
        Map<String,List<String>> mapAllPath =getAllPath();
        //输出出发站序号为1，终点站序号为24的的所有路线列表
        System.out.println(mapAllPath.get("1-24"));
    }
}
