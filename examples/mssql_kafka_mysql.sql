-- kafka数据源
CREATE TABLE jizy_flink_kafka (
                                  after_createtime bigint ,
                                  after_id varchar,
                                  after_updatetime bigint,
                                  after_user varchar
) WITH (
      type = 'kafka',
      bootstrapServers = '192.168.204.117:9092',
      zookeeperQuorum = '192.168.204.117:2181',
      offsetReset = 'latest',
      topic = 'jizy_flink_mssql',
      timezone = 'Asia/Shanghai'
      );

-- mysql目标数据源
CREATE TABLE jizy_flink_mysql (
                                  id varchar,
                                  createtime timestamp,
                                  updatetime timestamp,
                                  user varchar
) WITH (
      type = 'mysql',
      url = 'jdbc:mysql://192.168.209.246:3306/test_flinkx?useSSL=false',
      userName = 'root',
      password = 'Infra5@Gep0int',
      tableName = 'jizy_flink_copy'
      );

-- 实际执行的sql
insert into jizy_flink_mysql(createtime, updatetime, id,`user`) select TO_TIMESTAMP(FROM_UNIXTIME(after_createtime / 1000, 'yyyy-MM-dd HH:mm:ss')) as createtime, TO_TIMESTAMP(FROM_UNIXTIME(after_updatetime / 1000, 'yyyy-MM-dd HH:mm:ss')) as updatetime, after_id as id, after_user as `user` from jizy_flink_kafka