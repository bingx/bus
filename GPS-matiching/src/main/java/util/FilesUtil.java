package util;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * JAVA遍历一个文件夹中的所有文件
 * @author zhengshaorong
 */
public class FilesUtil {
	private List<String> absolutePaths = new LinkedList<String>();
	/*
	 * 通过递归得到某一路径下所有的目录及其文件(记得必须得传目录，不能传文件)
	 */
	public List<String> getFiles(String filePath) {
		File root = new File(filePath);
		File[] files = root.listFiles();
		if(files==null){//如果传进来的是文件，而不是目录
			absolutePaths.add(filePath);
			return absolutePaths;
		}
		for (File file : files) {
			if (file.isDirectory()) {
				getFiles(file.getAbsolutePath());
			} else {// 默认为没有目录，只有文件
				absolutePaths.add(file.getAbsolutePath());
			}
		}
		return absolutePaths;
	}
	public static int getPathDate(List<String> pathList) {
		String path = pathList.get(0);//链表第一个元素
		String[] busArr = path.split(Cfg.pathMap.get(Cfg.SEPARATOR));
		String busName = busArr[busArr.length - 1];//取文件名
//        int start = busName.length() - 6;//window环境以.txt为后缀
//        int end = busName.length() - 4;
		int start = busName.length() - 2;//linux环境无后缀
		int end = busName.length();
		int date = Integer.parseInt(busName.substring(start, end));//取日
		return date;
	}
	public static void main(String[] args) {
		FilesUtil fu=new FilesUtil();
		List<String> list=fu.getFiles("C:\\Users\\sibat\\Desktop\\jjkkj");
	}
}
