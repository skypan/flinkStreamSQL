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
        topic = 'test_bb'
        );

CREATE TABLE pres30302700002
(
    RowGuid         varchar,
    SYNC_SIGN       varchar,
    SYNC_ERROR_DESC text,
    OperateType     varchar,
    xxdz            varchar,
    xzqhdm          varchar,
    gddh            varchar,
    tjjdm           varchar,
    qrjb            varchar,
    ztqh            varchar,
    zt              varchar,
    zyywhd1         varchar,
    fddbr           varchar,
    zzjgdm          varchar,
    shxydm          varchar,
    kynf            varchar,
    djzclx          varchar,
    qygm_11         varchar,
    xydm            varchar,
    xydm_11         varchar,
    kyyf            varchar,
    xxmc            varchar,
    uploadtime      varchar,
    zjgcdb_scsj     varchar,
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
    SYNC_SIGN       varchar,
    SYNC_ERROR_DESC text,
    OperateType     varchar,
    SYNC_Date       timestamp,
    xxdz            varchar,
    xzqhdm          varchar,
    gddh            varchar,
    tjjdm           varchar,
    qrjb            varchar,
    ztqh            varchar,
    zt              varchar,
    zyywhd1         varchar,
    fddbr           varchar,
    zzjgdm          varchar,
    shxydm          varchar,
    kynf            varchar,
    djzclx          varchar,
    qygm_11         varchar,
    xydm            varchar,
    xydm_11         varchar,
    kyyf            varchar,
    xxmc            varchar,
    uploadtime      varchar,
    zjgcdb_scsj     varchar,
    primary key (RowGuid),
    PERIOD FOR SYSTEM_TIME
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
        tableName = 'pres30302700003');

insert into pres30302700002(RowGuid, xxdz, uploadtime, OperateType, SYNC_SIGN,SYNC_ERROR_DESC,xzqhdm,gddh,tjjdm,qrjb,ztqh,zt,zyywhd1,fddbr,zzjgdm,shxydm,kynf,djzclx,qygm_11,xydm,xydm_11,kyyf,xxmc,zjgcdb_scsj)
select
       a.rowguid as RowGuid,
       a.xxdz,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') as uploadtime,
       if(a.OperateType is null, 'Insert', 'Update') as OperateType,
       b.SYNC_SIGN,
       b.SYNC_ERROR_DESC,
       b.xzqhdm,
       b.gddh,
       b.tjjdm,
       b.qrjb,
       b.ztqh,
       b.zt,
       b.zyywhd1,
       b.fddbr,
       b.zzjgdm,
       b.shxydm,
       b.kynf,
       b.djzclx,
       b.qygm_11,
       b.xydm,
       b.xydm_11,
       b.kyyf,
       b.xxmc,
       b.zjgcdb_scsj
    from pres30302700001 a left join pres30302700003 b on a.rowguid = b.RowGuid
