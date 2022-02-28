CREATE TABLE source
(
    id timestamp as column1,
    IDCARD VARCHAR
)
    WITH (
        type = 'kafka',
        bootstrapServers = '192.168.204.117:9092',
        zookeeperQuorum = '192.168.204.117:2181',offsetReset = 'earliest',timezone = 'Asia/Shanghai',topic = 'tmltopic--');

CREATE TABLE target
(
    Column1 timestamp
)
    WITH (
        type = 'mysql',
        url = 'jdbc:mysql://192.168.209.246:3306/th4.1_tgbmzyk?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai',
        password = 'Infra5@Gep0int',
        userName = 'root',
        tableName = 'thceshi2222',
        updateMode = 'APPEND',
        parallelism = '1',
        batchSize = '1000',
        batchWaitInterval = '1000');

CREATE TABLE weibiao
(
    IDCARD VARCHAR,
    primary key (IDCARD),
    PERIOD FOR SYSTEM_TIME
)
    WITH (
        type = 'oracle',
        url = 'jdbc:oracle:thin:@192.168.209.249:1521:orcl',
        schema = 'TMLJCSOURCE',
        password = 'admin123',
        userName = 'admin',
        cache = 'None',
        asyncPoolSize = '3',
        parallelism = '1',
        partitionedJoin = 'false',
        tableName = 'TMLBASEINFOTARGET1213');

insert into target
    select
           t1.column1 as column1
    from source t1
        left join
         weibiao FOR SYSTEM_TIME AS OF t1.PROCTIME AS t2
        on t1.IDCARD = t2.IDCARD