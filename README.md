> **代码更新 （7.6）**

 * 2nd version 代码目前最完全，拿到数据直接写tair //建议先用这个，效率不一定比3rd version差

 * 3rd version 在2nd version之上把计数map<minute, money>换成了ConcurrentHashMap,单独开线程定时写tair

> **问题列表**

 **未解决**

 * tair 本地环境没法插入数据          	//7.7 弄好本地tair

 * nimbus server 运行一段时间会崩掉	//用三台机器搭个集群，jstorm,zookeeper三台都有，rocketMQ, tair 只放在master上

 * 存储策略

    * 每拿到一条消息就写入tair, 问题是不必要的写入次数太多，每一分钟内的每个交易都写一次，优点是操作简单

    * 单独开一个线程定时去ConcurrentHashMap拿数据写tair, 问题是每次都要遍历map，可能没写完，有重复写，优点是写入次数相对少

    * 在方案二的基础上维护一个tag，标识它是否有更新，有更新的才写入tair

 **已解决**

 * ConsumeConcurrentlyContext not found  //使用mvn install 把缺失的jar补上即可

 * 精度 //计算时使用Long, 存储时除100.0转成double

 * consumeMessage(msgList, context) msgList size 一直是1 //使用consumer.setConsumeMessageBatchMaxSize，不是每来一次消息就唤醒listener，而是到一定数量批量处理

> **提交前注意**

* [官方要求](https://bbs.aliyun.com/read/286553.html)

* 修改RaceTopology里面的并行度和worker数目

* 修改RaceConfig里面的tair 本地配置为阿里云配置

* 可以注释掉部分log，减少I/O提高性能

* [官方给的性能调优建议](https://bbs.aliyun.com/read/287850.html?displayMode=1)

> **编译日志下载**

 * [日志下载](http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/424452my9i.tar.xz)

 * [日志说明：](https://bbs.aliyun.com/read/287102.html?spm=5176.bbsl254.0.0.FC5JU3)
