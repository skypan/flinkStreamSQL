CREATE TABLE testtopic (
                           data [ 0 ] .id INT AS id,
                           data [ 0 ] .NAME VARCHAR AS NAME,
                           data [ 0 ] .sex INT AS sex,
                           data [ 0 ] .age INT AS age
) WITH (
      TYPE = 'kafka',
      bootstrapServers = '192.168.204.117:9092',
      zookeeperQuorum = '192.168.204.117:2181',
      offsetReset = 'latest',
      timezone = 'Asia/Shanghai',
      topic = 'test_topic'
      );

CREATE TABLE usertarget (
                            id INT,
                            age INT,
                            NAME VARCHAR,
                            sex VARCHAR,
                            PRIMARY KEY (id)
) WITH (
      TYPE = 'mysql',
      url = 'jdbc:mysql://192.168.151.9:3306/epointdxp_target?rewriteBatchedStatements=true',
      PASSWORD = 'Gepoint',
      userName = 'root',
      tableName = 'usertarget',
      updateMode = 'UPSERT',
      parallelism = '1',
      batchSize = '1000',
      batchWaitInterval = '100'
      );

CREATE TABLE itemcode (
                          CODE VARCHAR,
                          codename VARCHAR,
                          PRIMARY KEY (CODE),
                          PERIOD FOR SYSTEM_TIME
) WITH (
      TYPE = 'mysql',
      url = 'jdbc:mysql://127.0.0.1:3306/epointdxp_source?rewriteBatchedStatements=true',
      PASSWORD = 'Gepoint',
      userName = 'root',
      CACHE = 'None',
      asyncPoolSize = '1',
      parallelism = '1',
      errorLimit = '1',
      partitionedJoin = 'false',
      tableName = 'codeitem'
      );


insert into usertarget SELECT
                           a.age,
                           a.id,
                           a.name,
                           b.codename AS sex
FROM
    testtopic a
        LEFT JOIN itemcode b ON a.sex = b.code