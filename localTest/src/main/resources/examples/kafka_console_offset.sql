CREATE TABLE kafka_source
(
    data[0].allfields.after_data.RowGuid.value varchar as rowguid
)
    WITH (type = 'kafka',groupId = 'g1',bootstrapServers = '192.168.204.117:9092',zookeeperQuorum = '192.168.204.117:2181',offsetReset = '{"1":0}',timezone = 'Asia/Shanghai',topic = 'cdctopic_uamau');

CREATE TABLE console_target
(
    RowGuid varchar
) WITH (
      type = 'console',
      parallelism = '1'
      );

insert into console_target
select *
from kafka_source;