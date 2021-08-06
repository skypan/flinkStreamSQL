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
    sex int,
    createtime timestamp
) WITH (
      type = 'mysql',
      url = 'jdbc:mysql://192.168.209.246:3306/test_flinkx?useSSL=false',
      userName = 'root',
      password = 'Infra5@Gep0int',
      tableName = 'tb_emp1'
);

-- 实际执行的sql
insert into tb_emp1_mysql select name, salary, sex, PROCTIME as createtime from tb_emp1_kafka