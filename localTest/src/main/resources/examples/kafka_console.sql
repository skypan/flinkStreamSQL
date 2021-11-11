CREATE TABLE kafka_atest1_copy
(
    row_id      int,
    rowguid     varchar,
    ProductName varchar,
    BrandName   varchar
) WITH (
      type = 'kafka',
      bootstrapServers = '127.0.0.1:9092',
      zookeeperQuorum = '127.0.0.1:2181',
      offsetReset = 'latest',
      topic = 'doris_test',
      --- topic ='mqTest.*',
      ---topicIsPattern='true',
      parallelism = '1'
      );

CREATE TABLE doris_atest1_copy
(
    row_id      int,
    rowguid     varchar,
    ProductName varchar,
    BrandName   varchar
) WITH (
      type ='console',
      parallelism ='1'
      );

insert into doris_atest1_copy
    select * from kafka_atest1_copy;