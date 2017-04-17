# bus
新建项目模版，使用gradle构建，支持java与scala混编
 具体怎么新建子项目见[这里](https://github.com/konvish/template/wiki/%E5%88%9B%E5%BB%BA%E5%AD%90%E9%A1%B9%E7%9B%AE)
 外层build.gradle是项目的全局配置
 dependencies.gradle是用来版本控制的
 setting.gradle是放置子项目信息的

---
目前项目包含三个模块:'bus-common'、'metro-common'、'taxi-common'对应的是公交模块、地铁模块、出租车模块

>
*项目需要的环境安装java，scala，gradle<br>
*各个子项目需要添加各自的依赖直接在各自的项目区build.gradle里面的dependencies添加
