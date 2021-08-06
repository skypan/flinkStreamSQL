-- kafka数据源
CREATE TABLE attest_kafka (
                              after_ID varchar,
                              after_CONTENT text
) WITH (
      type = 'kafka',
      bootstrapServers = '192.168.204.117:9092',
      zookeeperQuorum = '192.168.204.117:2181',
      offsetReset = 'latest',
      topic = 'cdc_attest',
      timezone = 'Asia/Shanghai',
      sourcedatatype = 'json'
      );

-- mysql目标数据源
CREATE TABLE attest_mysql (
                                  id varchar,
                                  content text,
                                  primary key (id)
) WITH (
      type = 'mysql',
      url = 'jdbc:mysql://192.168.209.246:3306/attest?useSSL=false',
      userName = 'root',
      password = 'Infra5@Gep0int',
      tableName = 'testcdc_copy',
    updateMode = 'UPSERT'
      );

-- 实际执行的sql
insert into attest_mysql(id, content) select after_ID as id, after_CONTENT as content from attest_kafka