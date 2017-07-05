package cn.ODBackModel.utils;

import org.mozilla.intl.chardet.nsDetector;
import org.mozilla.intl.chardet.nsICharsetDetectionObserver;
import org.mozilla.intl.chardet.nsPSMDetector;

import java.io.*;
import java.util.Scanner;

/**
 * Created by hu on 2016/11/3.
 *
 * 利用InputStreamReader(InputStream in,Charset cs)来处理字符流时，charset不一定知道，所以需要利用firefox开源的字节流编码检测算法
 * 来检测编码方式。参考来源: http://www.oschina.net/question/54100_25358
 */
public class CodingDetector {

    private boolean found = false;
    private String result;
    private int lang;

    /**
     * 检测TXT文件编码
     */
    public static String checkTxtCode(String txtPath){
        String txtCode = "utf-8";
        try {

            FileInputStream in=new FileInputStream(new File(txtPath));
            //mainPkg=new CodingDetector().detectAllCharset(NoNameExUtil.class.getResourceAsStream("/Resource/站点编号对照表.csv"))[0];
            txtCode = new CodingDetector().detectAllCharset(in)[0];

            if(txtCode.equals("ASCII"))
                txtCode = "GB2312";
            in.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //        System.out.println(txtCode);
        return txtCode;
    }
    public String[] detectChineseCharset(InputStream in) throws IOException
    {
        lang = nsPSMDetector.CHINESE;
        String[] prob;
        nsDetector det = new nsDetector(lang);
        det.Init(new nsICharsetDetectionObserver()
        {
            public void Notify(String charset)
            {
                found = true;
                result = charset;
            }
        });
        BufferedInputStream imp = new BufferedInputStream(in);
        byte[] buf = new byte[1024];
        int len;
        boolean isAscii = true;
        while ((len = imp.read(buf, 0, buf.length)) != -1)
        {
            // Check if the stream is only ascii.
            if (isAscii)
                isAscii = det.isAscii(buf, len);
            // DoIt if non-ascii and not done yet.
            if (!isAscii)
            {
                if (det.DoIt(buf, len, false))
                    break;
            }
        }
        imp.close();
        in.close();
        det.DataEnd();
        if (isAscii)
        {
            found = true;
            prob = new String[]
                    {
                            "ASCII"
                    };
        } else if (found)
        {
            prob = new String[]
                    {
                            result
                    };
        } else
        {
            prob = det.getProbableCharsets();
        }
        return prob;
    }

    public  String[] detectAllCharset(String path) throws IOException
    {
        FileInputStream in=new FileInputStream(path);
        try
        {
            lang = nsPSMDetector.ALL;
            return detectChineseCharset(in);
        } catch (IOException e)
        {
            throw e;
        }

    }

    public  String[] detectAllCharset(File path) throws IOException
    {
        FileInputStream in=new FileInputStream(path);
        try
        {
            lang = nsPSMDetector.ALL;
            return detectChineseCharset(in);
        } catch (IOException e)
        {
            throw e;
        }

    }

    public  String[] detectAllCharset(InputStream in) throws IOException
    {
        try
        {
            lang = nsPSMDetector.ALL;
            return detectChineseCharset(in);
        } catch (IOException e)
        {
            throw e;
        }

    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String path="C:\\Users\\chuanglong\\Desktop\\frequency.csv";
        try {

            String code=new CodingDetector().detectAllCharset(new File(path))[0];
            //System.out.println();
            Scanner scan=new Scanner(new FileInputStream(path),code);
            while(scan.hasNext())
            {
                System.out.println(scan.nextLine());
            }
            scan.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
