package cn.ODBackModel.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hu on 2016/11/3.
 *
 *  一些功能函数
 */
public class FunctionsUtils {

    /**
     *  检测字符串 str 是否含有中文
     */
    public static boolean isContainsChinese(String str) {

        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        } else return false;
    }
}
