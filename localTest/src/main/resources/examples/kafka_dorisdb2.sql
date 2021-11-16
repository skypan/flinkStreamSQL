CREATE TABLE testfordoris
(
    data[0].rowguid varchar as rowguid
)
    WITH (
        type = 'kafka',
        groupId = 'group1',
        bootstrapServers = '192.168.204.117:9092',
        zookeeperQuorum = '192.168.204.117:2181',
        offsetReset = 'latest',
        timezone = 'Asia/Shanghai',
        topic = 'gcb_test');

CREATE TABLE test
(
    rowguid varchar
)
    WITH (
        type = 'dorisdb',
        url = 'jdbc:mysql://192.168.186.66:9030/dorisdb_test?rewriteBatchedStatements=true',
        connector = 'doris',
        jdbcUrl = 'jdbc:mysql://192.168.186.66:9030',
        loadUrl = '192.168.186.66:8030',
        databaseName = 'dorisdb_test',
        tableName = 'testtable_gucb',
        userName = 'test_user',
        password = '11111',
        sink.properties.strip_outer_array = 'true',
        parallelism = '1',
        sink.buffer-flush.interval-ms = '1000',
        sink.properties.format = 'json',
        sink.properties.column_separator = '\\x01',
        sink.properties.row_delimiter = '\\x02',
        fastCheck = 'false');


insert into test
select *
from testfordoris