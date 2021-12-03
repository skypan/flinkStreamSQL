CREATE TABLE source
(
    rowguid     varchar as rowguid,
    ProductName varchar as ProductName,
    BrandName   varchar as BrandName,
    row_id      varchar as row_id
)
    WITH (type = 'rabbitmq',host = '192.168.209.246',port = '5672',username = 'epoint',password = 'Infra5@Gep0int',queue = 'zwj_test1',parallelism = '1');

CREATE TABLE target_mq
(
    rowguid     varchar as rowguid,
    ProductName varchar as ProductName,
    BrandName   varchar as BrandName,
    row_id      varchar as row_id
)
    WITH (type = 'rabbitmq',host = '192.168.209.246',port = '5672',username = 'epoint',password = 'Infra5@Gep0int',queue = 'zwj_test2',parallelism = '1');


insert into target_mq
select *
from source;