# flinkSql demo
## 源数据源
 源数据只支持kafka
 
## 目标数据源
 目标数据源支持mysql、oracle、sqlserver、kafka等
 
## 测试步骤
### 下载源码包
 上github下载FlinkStreamSql的源码
 地址：https://gitee.com/fly_l/flinkStreamSQL.git  
 如果网络可以，也可以上github上下载
 
### 编译以及打包
 源码包现在之后，利用maven命令进行打包，打包命令如下  
 ```
 mvn clean package -Dmaven.test.skip
```
### 目录结构
  ```
    |
    |-----bin
    |     |--- submit.sh 任务启动脚本  
    |-----lib: launcher包存储路径，是任务提交的入口
    |     |--- sql.launcher.jar   
    |-----plugins:  插件包存储路径(mvn 打包之后会自动将jar移动到该目录下)  
    |     |--- core.jar
    |     |--- xxxsource
    |     |--- xxxsink
    |     |--- xxxside

```

### 编写sql
 以kafka同步到mysql为例子  
 新建源数据，目标数据，执行语句  
 ```sql
 -- kafka数据源
CREATE TABLE tb_emp1_kafka (
       name varchar,
       salary float,
       sex int
) WITH (
      type = 'kafka',
      bootstrapServers = '192.168.204.117:9092',
      zookeeperQuorum = '192.168.204.117:2181',
      offsetReset = 'latest',
      topic = 'flink_test',
      timezone = 'Asia/Shanghai'
      );

-- mysql目标数据源
CREATE TABLE tb_emp1_mysql (
       name varchar,
       salary float,
       sex int
) WITH (
      type = 'mysql',
      url = 'jdbc:mysql://192.168.209.246:3306/test_flinkx?useSSL=false',
      userName = 'root',
      password = 'Infra5@Gep0int',
      tableName = 'tb_emp1'
      );

-- 实际执行的sql
insert into tb_emp1_mysql select name, salary, sex from tb_emp1_kafka
```

### local模式启动
 本地启动模式，适用于本地调试
 ```
sh submit.sh
  -mode local
  -name local_test
  -sql F:\dtstack\stressTest\flinkStreamSql\stressTest.sql
  -localSqlPluginPath F:\dtstack\project\flinkStreamSQL\plugins
```
 参数说明：  
    mode    启动模式，local表示本地  
    name    任务名称  
    sql     sql脚本文件位置  
    localSqlPluginPath     本地插件包

### standalone模式启动
 提交到flink集群模式启动，以192.168.209.246为例子
 ```
./bin/submit.sh -mode standalone -sql ./examples/kafka_to_mysql.sql -name kafka_to_mysql.sql -localSqlPluginPath $FLINK_STREAM_SQL/sqlplugins -remoteSqlPluginPath  $FLINK_STREAMSQL/sqlplugins -flinkconf $FLINK_HOME/conf  -pluginLoadMode classpath -confProp {\"logLevel\":\"info\"}
```
 参数说明
 mode   启动模式，standalone模式需要  
 sql    提交的任务脚本  
 name   任务名称  
 localSqlPluginPath     本地插件目录  
 remoteSqlPluginPath    集群模式下插件目录  
 flinkconf  flink集群的配置文件所在目录
 pluginLoadMode 插件加载模式, standalone模式下必须配置为classpath
 confProp   配置说明
 
 
 