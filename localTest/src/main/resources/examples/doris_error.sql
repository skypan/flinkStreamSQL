CREATE TABLE M_MYSB_HZ_SJGDXX
(
    data[0].allfields.after_data.RegisterTime.value timestamp as gddjsj,
    data[0].allfields.after_data.RqstAreacode.value varchar as sjfsdxzqhbh,
    data[0].allfields.after_data.RqstType.value varchar as sqlxbm,
    data[0].allfields.after_data.gdbh.value varchar as gdbh
)
    WITH (
        type = 'kafka',
        bootstrapServers = '192.168.204.117:9092',
        zookeeperQuorum = '192.168.204.117:2181',
        offsetReset = 'latest',
        timestampoffset = '1641001742000',
        timezone = 'Asia/Shanghai',topic = 'cdctopic_oqchw');

CREATE TABLE M_MYSB_HZ_SJGDXX2
(
    gdbh        varchar,
    gddjsj      datetime,
    sjfsdxzqhbh varchar,
    sqlxbm      varchar
)
    WITH (
        type = 'dorisdb',
        url = 'jdbc:mysql://192.168.186.66:9030/dorisdb_test?rewriteBatchedStatements=true',
        connector = 'doris',
        jdbcUrl = 'jdbc:mysql://192.168.186.66:9030',
        loadUrl = '192.168.186.66:8030',
        databaseName = 'dorisdb_test',
        tableName = 'M_MYSB_HZ_SJGDXX2',
        userName = 'test_user',
        password = '11111',
        sink.properties.strip_outer_array = 'true',
        parallelism = '1',
        sink.buffer-flush.interval-ms = '1000',
        sink.properties.format = 'json',
        sink.properties.column_separator = '\\x01',
        sink.properties.row_delimiter = '\\x02',
        fastCheck = 'false'
        );

insert into M_MYSB_HZ_SJGDXX2 select * from M_MYSB_HZ_SJGDXX