Hippo
====================

hippo  是一个高性能，分布式，可扩展，高可靠的key/value结构存储系统！纯java开发k/v解决方案。<br>
Features:<br>
- 提供统一存储引擎接口， 很容易接入不同的存储实现， 现有mdb，  ldb的存储引擎。
- 实现了一个分布式k/v的存储框架， 有完备的集群， 分布式， 数据迁移， 数据复制的方案 5w+
- 具有高可用， 高容错， 保证缓存命中率特点的分布式系统
- 支持单机， 主备， 集群三种方式
- 支持丰富的数据类型（List， set, bitSet, Map）
－ 支持并发控制和原子计数
- 整个系统纯java编写， 使用堆外内存， 自实现了内存的管理， 避免gc
－ 客户端提供统一API， 自动的负载和容错



## 使用示例 ##

环境要求：JDK 6+
```xml
<dependency>
      <groupId>com.pinganfu.hippo</groupId>
      <artifactId>hippo.client</artifactId>
      <version>1.0.7</version>
</dependency>

#### Qucik Start ####
<!-- 提供spring -->
<bean id="hippoConnector" class="com.＊.hippo.client.HippoConnector">
  <property name="zookeeperUrl" value="192.168.1.98:2181,192.168.1.99:2181,192.168.1.100:2181" />
  <property name="clusterName" value="dev_cluster" />    - 此处请参考Hippo环境说明中的集群名字
</bean>
<bean id="client" class="com.＊.hippo.client.DefaultHippoClient" init-method="start" destroy-method="stop">
  <property name="connector" ref="hippoConnector" />
</bean>

<!-- API使用 -->
HippoConnector cc = new HippoConnector();
cc.setZookeeperUrl("192.168.1.98:2181,192.168.1.99:2181,192.168.1.100:2181");  // 各环境对应的Zookeeper地址
cc.setClusterName("ccluster-dev");                                              // 各环境对应的缓存集群名

DefaultHippoClient client = DefaultHippoClient.createClient(cc);    // 保持单例
client.start();                                      // 只需启动一次
／／client。stop();
```


## 性能测试 ##
请看性能报告



