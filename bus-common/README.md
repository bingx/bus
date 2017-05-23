调用Java类的技巧<br>
在引入写好的java包的时候很多时候，Spark Sql在进行Encoder的时候
都会出错，要么是Java没有实现序列化，没有实现序列化的时候，我们可以
写个继承类然后实现序列化，有序列化就跳过；
序列化完之后就需要我们自己写隐式转换，对java类进行encoder<br>
implicit val mapEncoder = Encoders.bean(classOf[Person])
---
例子<br>
public class Child{
    private String childName;

    public Child() {
    }

    public Child(String childName) {
        this.childName = childName;
    }

    public String getChildName() {
        return childName;
    }

    public void setChildName(String childName) {
        this.childName = childName;
    }
}

---
对child类进行加工ExChild实现序列化<br>
public class ExChild extends Child implements Serializable {
    public ExChild() {
    }

    public ExChild(String childName) {
        super(childName);
    }
}

---
替换成序列化的成员<br>
public class Person implements Serializable{
    private String name;
    private String age;
    //private Child child;
    private ExChild child;

    public Person() {
    }

    public Person(String name, String age, ExChild child) {
        this.name = name;
        this.age = age;
        this.child = child;
    }

    public Child getChild() {
        return child;
    }

    public void setChild(ExChild child) {
        this.child = child;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", child=" + child +
                '}';
    }
}