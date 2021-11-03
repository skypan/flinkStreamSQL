CREATE
    TABLE pres30302700001
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
        tiemstampoffset = '1629162300000',
        timezone = 'Asia/Shanghai',
        topic = 'test_mem');

CREATE
    TABLE pres30302700003
(
    RowGuid     varchar,
    OperateType varchar,
    xxdz        varchar,
    uploadtime  varchar,
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
        batchSize = '10000',batchWaitInterval = '1000');


insert
into pres30302700003(RowGuid, xxdz, uploadtime, OperateType)
select rowguid                                               as RowGuid,
       xxdz,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') as uploadtime,
       if(OperateType is null, 'Insert', 'Update')           as OperateType
from pres30302700001
