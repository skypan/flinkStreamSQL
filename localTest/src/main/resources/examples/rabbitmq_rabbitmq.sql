CREATE TABLE rabbitmq_atest1_copy
(
    row_id      int,
    rowguid     varchar,
    ProductName varchar,
    BrandName   varchar
) WITH (
      type = 'rabbitmq',
      host = '192.168.209.246',
      port = '5672',
      username = 'epoint',
      password = 'Infra5@Gep0int',
      queue = 'atest1_copy',
      parallelism = '1'
      );

CREATE TABLE rabbitmq_atest1_copy2
(
    row_id      int,
    rowguid     varchar,
    ProductName varchar,
    BrandName   varchar
) WITH (
      type = 'rabbitmq',
      host = '192.168.209.246',
      port = '5672',
      username = 'epoint',
      password = 'Infra5@Gep0int',
      queue = 'atest1_copy2',
      parallelism = '1'
      );

insert into rabbitmq_atest1_copy2
select * from rabbitmq_atest1_copy;