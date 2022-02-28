CREATE TABLE thceshi_kafka_c1
(
    data[0].citizen varchar as citizen,
    data[0].birthdate varchar as birthdate,
    data[0].address varchar as address,
    data[0].comment1 varchar as comment1,
    data[0].sex varchar as sex,
    data[0].creditscore varchar as creditscore,
    data[0].photo varchar as photo,
    data[0].telephone varchar as telephone,
    data[0].regdatetime varchar as regdatetime,
    data[0].bloodtype varchar as bloodtype,
    data[0].housingareas varchar as housingareas,
    data[0].birthplace varchar as birthplace,
    data[0].idcard varchar as idcard,
    data[0].name varchar as name,
    data[0].personid varchar as personid,
    data[0].updatetime varchar as updatetime,
    data[0].age varchar as age
)
    WITH (
        type = 'kafka',
        bootstrapServers = '192.168.204.117:9092',
        zookeeperQuorum = '192.168.204.117:9092',offsetReset = 'latest',timezone = 'Asia/Shanghai',topic = 'thceshi_kafka_c1');

CREATE TABLE thceshi_yichang_c
(
    citizen      varchar,
    birthdate    varchar,
    address      varchar,
    comment1     varchar,
    sex          varchar,
    creditscore  varchar,
    photo        varchar,
    telephone    varchar,
    regdatetime  varchar,
    bloodtype    varchar,
    housingareas varchar,
    birthplace   varchar,
    idcard       varchar,
    name         varchar,
    personid     varchar,
    updatetime   varchar,
    age          varchar,
    errormsg     varchar
)
    WITH (type = 'mysql',url = 'jdbc:mysql://192.168.209.246:3306/th4.1_tgbmzyk?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai',password = 'Infra5@Gep0int',userName = 'root',tableName = 'thceshi_yichang_c',updateMode = 'APPEND',parallelism = '1',batchSize = '1000',batchWaitInterval = '1000');
CREATE TABLE thceshi_qingxi_c
(
    data[0].citizen varchar as citizen,
    data[0].birthdate varchar as birthdate,
    data[0].address varchar as address,
    data[0].comment1 varchar as comment1,
    data[0].sex varchar as sex,
    data[0].creditscore varchar as creditscore,
    data[0].photo varchar as photo,
    data[0].telephone varchar as telephone,
    data[0].regdatetime varchar as regdatetime,
    data[0].bloodtype varchar as bloodtype,
    data[0].housingareas varchar as housingareas,
    data[0].birthplace varchar as birthplace,
    data[0].idcard varchar as idcard,
    data[0].name varchar as name,
    data[0].personid varchar as personid,
    data[0].updatetime varchar as updatetime,
    data[0].age varchar as age
)
    WITH (type = 'kafka',bootstrapServers = '192.168.204.117:9092',zookeeperQuorum = '192.168.204.117:9092',topic = 'thceshi_qingxi_c');


insert into thceshi_qingxi_c
select a.citizen,
       a.birthdate,
       a.address,
       a.comment1,
       a.sex,
       a.creditscore,
       a.photo,
       a.telephone,
       a.regdatetime,
       a.bloodtype,
       a.housingareas,
       a.birthplace,
       a.idcard,
       a.name,
       a.personid,
       a.updatetime,
       a.age
from (
         select a.citizen      citizen,
                a.birthdate    birthdate,
                a.address      address,
                a.comment1     comment1,
                a.sex          sex,
                a.creditscore  creditscore,
                a.photo        photo,
                a.telephone    telephone,
                a.regdatetime  regdatetime,
                a.bloodtype    bloodtype,
                a.housingareas housingareas,
                a.birthplace   birthplace,
                a.idcard       idcard,
                a.name         name,
                a.personid     personid,
                a.updatetime   updatetime,
                a.age          age
         from thceshi_kafka_c1 a
         where 1 = 1
     ) a
where (
          (
                  (a.age is null)
                  or (
                          a.age >= 100
                          and a.age <= 200
                      )
              )
          );
insert into thceshi_yichang_c
select a.citizen,
       a.birthdate,
       a.address,
       a.comment1,
       a.sex,
       a.creditscore,
       a.photo,
       a.telephone,
       a.regdatetime,
       a.bloodtype,
       a.housingareas,
       a.birthplace,
       a.idcard,
       a.name,
       a.personid,
       a.updatetime,
       a.age,
       rangeReason as errorMsg
from (
         select a.citizen,
                a.birthdate,
                a.address,
                a.comment1,
                a.sex,
                a.creditscore,
                a.photo,
                a.telephone,
                a.regdatetime,
                a.bloodtype,
                a.housingareas,
                a.birthplace,
                a.idcard,
                a.name,
                a.personid,
                a.updatetime,
                a.age,
                case
                    when (a.age is not null)
                        and (
                                 a.age < 100
                                 or a.age > 200
                             ) then '[age]字段值不在"100 ~ 200"值域范围内！'
                    else null end as rangeReason
         from (
                  select a.citizen      citizen,
                         a.birthdate    birthdate,
                         a.address      address,
                         a.comment1     comment1,
                         a.sex          sex,
                         a.creditscore  creditscore,
                         a.photo        photo,
                         a.telephone    telephone,
                         a.regdatetime  regdatetime,
                         a.bloodtype    bloodtype,
                         a.housingareas housingareas,
                         a.birthplace   birthplace,
                         a.idcard       idcard,
                         a.name         name,
                         a.personid     personid,
                         a.updatetime   updatetime,
                         a.age          age
                  from thceshi_kafka_c1 a
                  where 1 = 1
              ) a
         where (
                       (a.age is not null)
                       and (
                               a.age < 100
                               or a.age > 200
                           )
                   )
     ) a