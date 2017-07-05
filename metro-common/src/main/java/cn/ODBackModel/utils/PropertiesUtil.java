package cn.ODBackModel.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hu on 2016/11/3.
 *
 *  读取配置文件
 */
public class PropertiesUtil {

    //配置文件的路径
    private String configPath=null;

    /**
     * 配置文件对象
     */
    private Properties props=null;

    /**
     * 默认构造函数，用于sh运行，自动找到classpath下的config.properties。
     */
    public PropertiesUtil() throws IOException {

/*		String path1= System.getProperty("user.dir");
		int index1=path1.lastIndexOf(File.separator);
		String path2=path1.substring(0,index1);
		int index2=path2.lastIndexOf(File.separator);
		String path3=path2.substring(0,index2)+File.separator+"oracle.properties";*/

        this.configPath = "oracle.properties";
        //InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream("Resource/oracle.properties");

        InputStream in=new FileInputStream(this.configPath);
        props = new Properties();
        props.load(in);
        in.close();
    }

    public PropertiesUtil(String properties) throws IOException{
        configPath = properties;
        InputStream in=this.getClass().getClassLoader().getResourceAsStream(configPath);
        props = new Properties();
        props.load(in);
        in.close();
    }

    public String readValue(String key) throws IOException {
        return  props.getProperty(key);
    }

    public Map<String,String> readAllProperties() throws FileNotFoundException,IOException  {
        //保存所有的键值
        Map<String,String> map=new HashMap<String,String>();
        Enumeration en = props.propertyNames();
        while (en.hasMoreElements()) {
            String key = (String) en.nextElement();
            String Property = props.getProperty(key);
            map.put(key, Property);
        }
        return map;
    }


//    public void setValue(String key,String value) throws IOException {
//        Properties prop = new Properties();
//        InputStream fis = this.getClass().getClassLoader().getResourceAsStream(this.configPath);
//        // 从输入流中读取属性列表（键和元素对）
//        prop.load(fis);
//        // 调用 Hashtable 的方法 put。使用 getProperty 方法提供并行性。
//        // 强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
//        OutputStream fos = new OutputStream(new FileOutputStream());
//
//        // prop.setProperty(key, value);
//        prop.put(key, value);
//        // 以适合使用 load 方法加载到 Properties 表中的格式，
//        // 将此 Properties 表中的属性列表（键和元素对）写入输出流
//        prop.store(fos,"last update");
//        //关闭文件
//        fis.close();
//        fos.close();
//    }

    public static void main(String[] args) {
//        PropertiesUtil p;
//        try {
//            p = new PropertiesUtil();
////            // System.out.println(p.readAllProperties());
////            p.setValue("ip", "172.21.4.228");
////            p.setValue("port", "1521");
////            p.setValue("dbName", "ORCL");
////            p.setValue("passWord", "123456");
////            p.setValue("userName", "szt");
//
//            System.out.println(p.readValue("scalePlanTable"));
//
//            p.setValue("scalePlanTable", "sys.qf_par_clearing_scale_plan");
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }
}
