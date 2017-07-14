package util;

import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * @author zhengshaorong
 */
public class Cfg {
	public static final String SEPARATOR = "separator";
	public static final String BUS_DIRECTORY_PATH = "bus.directory.path";
	public static final String TAXI_DIRECTORY_PATH = "taxi.directory.path";
	public static final String E6_DIRECTORY_PATH = "e6.directory.path";
	public static final String COACH_DIRECTORY_PATH = "coach.directory.path";
	//out path directory
	public static final String E6_OUT="e6.out.path";
	public static final String BUS_OUT="bus.out.path";
	public static final String TAXI_OUT="taxi.out.path";
	public static final String PROVROAD_OUT="ProvRoad.out.path";

	public static Map<String, String> pathMap = new HashMap<String, String>();

	static {
		ResourceBundle bundle = ResourceBundle.getBundle("config");
		Enumeration<String> eum = bundle.getKeys();
		while (eum.hasMoreElements()) {
			String key = eum.nextElement();
			try {
				pathMap.put(key, new String(bundle.getString(key).getBytes("ISO-8859-1"), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}
}
