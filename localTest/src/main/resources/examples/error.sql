CREATE TABLE tmlysb1221
(
    data[0].idcard varchar as idcard,
    data[0].name varchar as name,
    data[0].age int as age
)
    WITH (
        type = 'kafka',
        groupId = '1',
        bootstrapServers = '192.168.204.117:9092',
        zookeeperQuorum = '',
        offsetReset = 'latest',
        timezone = 'Asia/Shanghai',
        topic = 'tmltopic_tmltest_two');

CREATE TABLE tmlysb122111
(
    idcard varchar,
    name   varchar,
    age    int,
    primary key (idcard)
)
    WITH (
        type = 'mysql',
        url = 'jdbc:mysql://192.168.209.246:3306/4.1tmlsjzl?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai',
        password = 'Infra5@Gep0int',
        userName = 'root',
        tableName = 'tmlbaseinfo1221',
        updateMode = 'UPSERT',parallelism = '1',batchSize = '1000',batchWaitInterval = '1000');


insert into tmlysb122111
select *
from tmlysb1221 a