# 问题产生的背景

- 在spark on yarn模式的客户端，想直接通过spark-submit或spark-sql提交HQL脚本或SQL语句
- spark-sql不支持cluter模式，需要在本地启动driver，占用内存较大
- 若是在一个作业调度系统中，需要减少本地内存的使用

# 解决思路

如果有spark coding能力，可以实现一个jar包专门接收HQL/SQL参数，采用spark-submit提交到集群

自己实现的jar包，处理起sql可能功能有限，支持变量，session管理等诸多问题，那我们可不可以既用spark-sql又用cluster模式呢？答案是可以，下面是分析过程。

# 编译&打包

```
 mvn clean scala:compile compile package
```

详情参考：
```
作者：kazke
链接：https://juejin.cn/post/7056334152408236046
来源：稀土掘金
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
```
