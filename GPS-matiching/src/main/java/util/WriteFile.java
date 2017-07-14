package util;

import com.sibat.traffic.GlobalInfo;

import java.io.*;

/**
 * Created by User on 2017/5/18.
 */
public class WriteFile {

    /**
     * B方法追加文件：使用FileWriter
     */
    public static void appendMethodB(String fileName, String content) {
        try {

            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(content+"\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void writeToFile(String content ) throws IOException {
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        String fileName = GlobalInfo.writeFileName;
        String rootPath = GlobalInfo.outPath;
        String path = rootPath +"/"+ fileName + ".txt";
        File folder = new File(rootPath);
        File file = new File(path);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        if(!file.exists()){
            file.createNewFile();
        }
        try {
            fos = new FileOutputStream(file, true);
            bos = new BufferedOutputStream(fos);
            content=content+"\n";
            bos.write(content.getBytes());
            bos.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
