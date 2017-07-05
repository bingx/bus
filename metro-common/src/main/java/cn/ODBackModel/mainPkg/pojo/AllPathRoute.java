package cn.ODBackModel.mainPkg.pojo;

/**
 * Created by hu on 2016/11/3.
 */
public class AllPathRoute {

    public String string;
    public String o2d;

    public AllPathRoute(){}

    public AllPathRoute(String string,String o2d){
        this.string =string;
        this.o2d =o2d;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public String getO2d() {
        return o2d;
    }

    public void setO2d(String o2d) {
        this.o2d = o2d;
    }
}
