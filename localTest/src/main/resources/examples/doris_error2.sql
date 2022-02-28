CREATE TABLE source
(
    gdbh        varchar,
    gddjsj      timestamp,
    sjfsdxzqhbh varchar,
    sqlxbm      varchar
)
    WITH (
        type = 'kafka',
        bootstrapServers = '192.168.204.117:9092',
        zookeeperQuorum = '192.168.204.117:2181',
        offsetReset = 'latest',
        timezone = 'Asia/Shanghai',
        topic = 'fzhcs');

CREATE TABLE M_MYSB_HZ_SJGDXX2
(
    gdbh        varchar,
    gddjsj      datetime,
    sjfsdxzqhbh varchar,
    sqlxbm      varchar,
    primary key (gdbh)
)
    WITH (
        type = 'dorisdb',
        url = 'jdbc:mysql://192.168.186.66:9030/dorisdb_test?rewriteBatchedStatements=true',
        jdbcUrl = 'jdbc:mysql://192.168.186.66:9030/dorisdb_test?rewriteBatchedStatements=true',
        loadUrl = '192.168.186.66:8030',
        databaseName = 'dorisdb_test',
        tableName = 'M_MYSB_HZ_SJGDXX2',userName = 'test_user',password = '11111',sink.properties.strip_outer_array = 'true',parallelism = '1',sink.buffer-flush.interval-ms = '1000',sink.properties.format = 'json',sink.properties.column_separator = '\\x01',sink.properties.row_delimiter = '\\x02',fastCheck = 'false');


insert into M_MYSB_HZ_SJGDXX2
select *
from source