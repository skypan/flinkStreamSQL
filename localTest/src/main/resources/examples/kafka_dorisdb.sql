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
      type = 'dorisdb',
      url = 'jdbc:mysql://192.168.186.66:9030/dorisdb_test?useSSL=false',
      userName = 'test_user',
      password = '11111',
      tableName = 'atest1_copy',
      parallelism = '1',
      databaseName = 'dorisdb_test',
      loadUrl = '192.168.186.66:8030',
      jdbcUrl = 'jdbc:mysql://192.168.186.66:9030',
      sink.properties.strip_outer_array = 'true',
      sink.buffer-flush.interval-ms = '1000',
      sink.properties.format = 'json',
      sink.properties.column_separator = '\\\\x01',
      sink.properties.row_delimiter = '\\\\x02',
      fastCheck = 'false'
      );

insert into doris_atest1_copy
    select * from kafka_atest1_copy;