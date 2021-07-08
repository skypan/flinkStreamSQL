-- kafka数据源
CREATE TABLE jizy_flink_kafka (
    data[0].allfields.after_data.createtime.value timestamp as createtime,
    data[0].allfields.after_data.id.value varchar as id,
    data[0].allfields.after_data.updatetime.value timestamp as updatetime,
    data[0].allfields.after_data.user.value varchar as user
) WITH (
      type = 'kafka',
      bootstrapServers = '192.168.204.117:9092',
      zookeeperQuorum = '192.168.204.117:2181',
      offsetReset = 'earliest',
      topic = 'jizy_flink',
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
      tableName = 'jizy_flink'
);

-- 实际执行的sql
insert into jizy_flink_mysql select * from jizy_flink_kafka