### Flink yarn HA 部署

#### 源码build

  这里以`flink-1.3.2`为例

- 进入flink github，选择相应的分支，下载相应的源码

   Branch-->branches/Tags，选择相应的`tag`，点击右侧`Clone and download`，下载对应的zip文件

- 解压文件
   $ unzip flink-release-1.3.2.zip

- 指定`scala`版本

   scala默认版本为`2.10`，如果平台使用`2.11`，则需要利用脚本调整flink的scala版本

   `注：有些flink高版本，可能不需要指定scala 2.11`

   $ cd flink-release-1.3.2

   $ tools/change-scala-version.sh 2.11

- build

   - 指定hadoop版本进行build

     $ mvn clean install -DskipTests

   - 指定hadoop厂商及版本进行build


      如果想让flink 运行在 `cdh`等商业hadoop上，需要指定厂商及版本进行build。下面以`cdh5.15.2`为例，编译可能需要30min左右。
   ```
     $ mvn clean install -DskipTests -Pvendor-repos -Dhadoop.version=2.6.0-cdh5.15.2
   ```

     build完毕后，在`flink-dist/target/flink-1.3.2-bin`中，即可看到编译好的`flink-1.3.2`安装目录，压缩后，即可得到安装包`flink-1.3.2.tar.gz`

   ```
   $ tar zcvf flink-1.3.2.tar.gz flink-1.3.2
   ```

   安装部署

- 将刚才编译好 安装包，拷贝安装目录下，解压并建立相应的软连接

  $ cp flink-1.3.2.zip   /opt/third

  $ cd /opt/third

  $ tar zxvf flink-1.3.2.tar.gz

  $ ln -s flink-1.3.2 flink

- 添加hadoop配置文件目录等相应的环境变量，并初始化 `flink`检查点及恢复目录

  - flink 要加载`HADOOP_CONF_DIR`或`YARN_CONF_DIR`的配置，以获取对应`Yarn ResourceManager`信息

    $ vim ~/.bash_profile

    ```
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    ```

    $ source ~/.bash_profile

    `注：在hadoop 配置目录下需要有hdfs-site.xml、core-site.xml、yarn-site.xml等hadoop基本配置文件`

  - 为`flink`准备相应的检查点及恢复目录

    $  hdfs dfs  -mkdir -p  /flink/recovery

    $  hdfs  dfs -mkdir /flink-checkpoints
 - 调整yarn的配置，使jobManager宕机后，能够拉起
   ```
    <property>
       <key>yarn.resourcemanager.am.max-attempts</key>
       <value>4</value>
    </property>
   ```

#### flink配置

- 调整flink配置，主要是zookeeper及Hdfs信息

     $ vim conf/flink-conf.yaml 

     ```
     # zookeeper
     high-availability: zookeeper
     # 状态存储，选择Hdfs
     state.backend: filesystem
     state.backend.fs.checkpointdir: hdfs:///flink-checkpoints
     # zookeeper quatum
     high-availability.zookeeper.quorum: zk1:2181,zk2:2181,zk3:2181
     high-availability.zookeeper.path.root: /flink
     high-availability.zookeeper.path.cluster-id: /dubhe_ns
     high-availability.zookeeper.storageDir: hdfs:///flink/recovery
     yarn.application-attempts: 10
     ```

- 验证，能够启动`yarn-session`，确保进程能够在yarn 集群上部署。

  bin/yarn-session.sh -n 1