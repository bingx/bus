package cn.probabilityModel;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.*;

import java.io.File;
import java.util.*;

/**
 * 解析输入的xml文件
 * 轨道交通路网信息.xml文件中的数据均存储在子节点的属性中，因此不需要考虑子节点内容
 * Created by wing1995 on 2017/7/3.
 */
public class XmlParse {
    //站点基本信息
    public static Integer stationCount; //118
    public static Integer transStation; //13
    public static Integer repeatAllStaitonCount; //131
    public static Integer adjacStaitonCount; //252
    public static Integer lineCount; //5
    public static Integer transLimitCount; //2

    //站点发车间隔,有几条线路就有几个元素
    public static List<Double> TrainInterval = new ArrayList<>();
    //列车发车速度
    public static List<Double> Speed = new ArrayList<>();
    //线路编码
    public static List<String> LineNoName = new ArrayList<>();

    //中距离时间被限制在60分钟以内
    public static double MiddleTravelLimit;
    //短距离时间被限制在30分钟以内
    public static double ShortTravelLimit;
    //长距离有效路径能够增加的时间为15分钟
    public static double LongAddLimit;
    //中距离有效路径能够增加的时间为10分钟
    public static double MiddleAddLimit;
    //短距离有效路径能够增加的时间为10分钟
    public static double ShortAddLimit;

    //正态模型参数
    public static double ShortTransPara;  //对应正态分布模型短途平峰时段的换乘时间放大系数3.390000
    public static double ShortSumPara;  //对应正态分布模型短途平峰时段的总出行时间的放大系数3.170000
    public static double ShortNormalPara;    //对应正态分布模型短途平峰时段的标准差系数12.610000
    public static double MiddleTransPara; //对应正态分布模型中途平峰时段的换乘时间放大系数3.850000
    public static double MiddleSumPara; //对应正态分布模型中途平峰时段的总出行时间的放大系数1.170000
    public static double MiddleNormalPara;   //对应正态分布模型中途平峰时段的标准差系数3.010000
    public static double LongTransPara;   //对应正态分布模型长途平峰时段的换乘时间放大系数2.450000
    public static double LongSumPara;   //对应正态分布模型长途平峰时段的总出行时间的放大系数1.920000
    public static double LongNormalPara;     //对应正态分布模型长途平峰时段的标准差系数7.050000

    //站点邻接信息，key-value对应于子节点detail的属性信息
    public static List<HashMap<String, String>> StationConnectInfor = new ArrayList<>();
    //换乘站步行时间
    public static List<Map<String, String>> TransStationWalkTime = new ArrayList<>();
    //OD点票价
    public static List<Map<String, String>> ODPrice = new ArrayList<>();
    //换乘站点所属线路
    public static List<Map<String, String>> TransStationBelongLine = new ArrayList<>();

    /**
     * 读取XML文件
     */
    public static void readXmlFile() {

        try {

            File file = new File("E:\\bus\\metro-common\\src\\main\\resources\\轨道交通路网信息.xml");

            DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            Node rootNode = dBuilder.parse(file).getChildNodes().item(0);

            if (rootNode.hasChildNodes()) { //若有子节点则输出子节点名称及属性
                getInfo(rootNode.getChildNodes());
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * 获取节点数据信息
     * @param nodeList 二级节点列表
     */
    private static void getInfo(NodeList nodeList) {

        //int secondNodesNum = nodeList.getLength();

        for (int count = 0; count < nodeList.getLength(); count++) { //loop in the second node

            Node secondNode = nodeList.item(count);

            // make sure it's element node.
            if (secondNode.getNodeType() == Node.ELEMENT_NODE) {

                // get the second node name and the third node name
                String secondNodeName = secondNode.getNodeName();
                NodeList thirdNodes = secondNode.getChildNodes();

                // 处理三级节点
                for (int thirdNodesNum = 0; thirdNodesNum < thirdNodes.getLength(); thirdNodesNum++) {

                    if (thirdNodes.item(thirdNodesNum).getNodeType() == Node.ELEMENT_NODE) {

                        if (thirdNodes.item(thirdNodesNum).hasAttributes()) {

                            //初始化字典
                            HashMap<String, String> StationConnectInforHashmap = new HashMap<>();
                            HashMap<String, String> TransStationWalkTimeHashmap = new HashMap<>();
                            HashMap<String, String> ODPriceHashmap = new HashMap<>();
                            HashMap<String, String> TransStationBelongLineHashmap = new HashMap<>();

                            // get the third node's attributes' names and values
                            NamedNodeMap thirdNodeAttributes = thirdNodes.item(thirdNodesNum).getAttributes();

                            for (int i = 0; i < thirdNodeAttributes.getLength(); i++) {

                                Node node = thirdNodeAttributes.item(i);
                                String attrName = node.getNodeName();
                                String attrValue = node.getNodeValue();

                                switch (secondNodeName) {
                                    case "BaseInformation":
                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
                                            case "StaitonCount":
                                                stationCount = Integer.parseInt(attrValue);
                                                break;
                                            case "TransStation":
                                                transStation = Integer.parseInt(attrValue);
                                                break;
                                            case "RepeatAllStaitonCount":
                                                repeatAllStaitonCount = Integer.parseInt(attrValue);
                                                break;
                                            case "AdjacStaitonCount":
                                                adjacStaitonCount = Integer.parseInt(attrValue);
                                                break;
                                            case "LineCount":
                                                lineCount = Integer.parseInt(attrValue);
                                                break;
                                            case "TransLimitCount":
                                                transLimitCount = Integer.parseInt(attrValue);
                                                break;
                                            case "TrainInterval":
                                                TrainInterval.add(Double.parseDouble(attrValue));
                                                break;
                                            case "Speed":
                                                Speed.add(Double.parseDouble(attrValue));
                                                break;
                                            case "LineNoName":
                                                LineNoName.add(attrValue);
                                                break;
                                            case "PathTimeSeparetor":
                                                switch (attrName) {
                                                    case "MiddleTravelLimit":
                                                        MiddleTravelLimit = Double.parseDouble(attrValue);
                                                        break;
                                                    case "ShortTravelLimit":
                                                        ShortTravelLimit = Double.parseDouble(attrValue);
                                                        break;
                                                }
                                            case "PathAddTimeSeparetor":
                                                switch (attrName) {
                                                    case "LongAddLimit":
                                                        LongAddLimit = Double.parseDouble(attrValue);
                                                        break;
                                                    case "MiddleAddLimit":
                                                        MiddleAddLimit = Double.parseDouble(attrValue);
                                                        break;
                                                    case "ShortAddLimit":
                                                        ShortAddLimit = Double.parseDouble(attrValue);
                                                }
                                            default:
                                                break;
                                        }
                                        break;
                                    case "ModelParaValue":
                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
                                            case "Short":
                                                switch (attrName) {
                                                    case "ShortNormalPara":
                                                        ShortNormalPara = Double.parseDouble(attrValue);
                                                        break;
                                                    case "ShortSumPara":
                                                        ShortSumPara = Double.parseDouble(attrValue);
                                                        break;
                                                    case "ShortTransPara":
                                                        ShortTransPara = Double.parseDouble(attrValue);
                                                        break;
                                                }
                                            case "Middle":
                                                switch (attrName) {
                                                    case "MiddleNormalPara":
                                                        MiddleNormalPara = Double.parseDouble(attrValue);
                                                        break;
                                                    case "MiddleSumPara":
                                                        MiddleSumPara = Double.parseDouble(attrValue);
                                                        break;
                                                    case "MiddleTransPara":
                                                        MiddleTransPara = Double.parseDouble(attrValue);
                                                }
                                            case "Long":
                                                switch (attrName) {
                                                    case "LongNormalPara":
                                                        LongNormalPara = Double.parseDouble(attrValue);
                                                        break;
                                                    case "LongSumPara":
                                                        LongSumPara = Double.parseDouble(attrValue);
                                                        break;
                                                    case "LongTransPara":
                                                        LongTransPara = Double.parseDouble(attrValue);
                                                }
                                            default:
                                                break;
                                        }
                                        break;
                                    case "StationConnectInfor":
                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
                                            case "Detail":
                                                switch (attrName) {
                                                    case "ClearLineNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "IsTrans":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "LineNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "NextClearLineNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "NextDirect":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "NextStatDis":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "NextStatNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "PreClearLineNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "PreDirect":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "PreStatDis":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "PreStatNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "ServiceFee":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "StatNo":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "StatStopTime1":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "StatStopTime2":
                                                        StationConnectInforHashmap.put(attrName, attrValue);
                                                        break;
                                                }
                                            default:
                                                break;
                                        }
                                        break;
                                    case "TransStationWalkTime":
                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
                                            case "WalkTime": {
                                                switch (attrName) {
                                                    case "FromLineNo":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "StationNo":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "ToLineNo":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "direction00":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "direction01":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "direction10":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "direction11":
                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
                                                        break;
                                                    default:
                                                        break;
                                                }
                                            }
                                        }
                                        break;
                                    case "ODPrice":
                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
                                            case "Price":
                                                switch (attrName) {
                                                    case "Ori":
                                                        ODPriceHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "Des":
                                                        ODPriceHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "Price":
                                                        ODPriceHashmap.put(attrName, attrValue);
                                                        break;
                                                }
                                        }
                                        break;
                                    case "TransStationBelongLine":
                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
                                            case "TransStationBelongLineInfor":
                                                switch (attrName) {
                                                    case "StationNo":
                                                        TransStationBelongLineHashmap.put(attrName, attrValue);
                                                        break;
                                                    case "BelongToLine":
                                                        TransStationBelongLineHashmap.put(attrName, attrValue);
                                                        break;
                                                    default:
                                                        break;
                                                }
                                        }
                                    break;
                                }
                            }
                            if (StationConnectInforHashmap.size() != 0) {
                                StationConnectInfor.add(StationConnectInforHashmap);
                            }
                            if (TransStationWalkTimeHashmap.size() != 0) {
                                TransStationWalkTime.add(TransStationWalkTimeHashmap);
                            }
                            if (ODPriceHashmap.size() != 0) {
                                ODPrice.add(ODPriceHashmap);
                            }
                            if (TransStationBelongLineHashmap.size() != 0) {
                                TransStationBelongLine.add(TransStationBelongLineHashmap);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 测试函数
     * @param args 参数
     */
    public static void main(String args[]) {
        readXmlFile();
        for (Map.Entry<String, String> map: StationConnectInfor.get(0).entrySet()) {
            System.out.println(map.getKey() + ":"  + map.getValue());
        }
        System.out.println();
        for (Map.Entry<String, String> map: TransStationWalkTime.get(0).entrySet()) {
            System.out.println(map.getKey() + ":"  + map.getValue());
        }
        System.out.println();
        for (Map.Entry<String, String> map: ODPrice.get(0).entrySet()) {
            System.out.println(map.getKey() + ":"  + map.getValue());
        }
        System.out.println();
        for (Map.Entry<String, String> map: TransStationBelongLine.get(0).entrySet()) {
            System.out.println(map.getKey() + ":"  + map.getValue());
        }
        System.out.println();
    }
}
