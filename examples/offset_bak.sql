CREATE TABLE pres30302700001
(
    data[0].RowGuid varchar as rowguid,
    data[0].xxdz varchar as xxdz,
    data[0].OperateType varchar as OperateType
)
WITH (
    type = 'kafka',
    bootstrapServers = '192.168.204.117:9092',
    zookeeperQuorum = '192.168.204.117:2181',
    offsetReset = 'timestamp',
    timestampoffset = '1629165423000',
    timezone = 'Asia/Shanghai',
    topic = 'test_cc'
);

CREATE TABLE pres30302700002
(
    RowGuid varchar,
    xxdz    varchar,
    primary key (RowGuid)
)
    WITH (
        type = 'mysql',
        url = 'jdbc:mysql://192.168.209.246:3306/dxp_cdc_ylcs?rewriteBatchedStatements=true',
        password = 'Infra5@Gep0int',
        userName = 'root',
        tableName = 'pres30302700003_2',
        updateMode = 'UPSERT',
        parallelism = '1',
        batchSize = '100',
        batchWaitInterval = '1000');

CREATE TABLE pres30302700003
(
    RowGuid         varchar,
    xxdz            varchar,
    OperateType     varchar,
    uploadtime      varchar,
    primary key (RowGuid)
    -- PERIOD FOR SYSTEM_TIME
)
    WITH (
        type = 'mysql',
        url = 'jdbc:mysql://192.168.209.246:3306/dxp_cdc_ylcs?rewriteBatchedStatements=true',
        password = 'Infra5@Gep0int',
        userName = 'root',
        cache = 'None',
        asyncPoolSize = '1',
        parallelism = '2',
        errorLimit = '1',
        partitionedJoin = 'false',
        tableName = 'pres30302700003_2');

insert into pres30302700003(RowGuid, xxdz, uploadtime, OperateType)
select rowguid as RowGuid, xxdz, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') as uploadtime, if(OperateType is null, 'Insert', 'Update') as OperateType
from pres30302700001
