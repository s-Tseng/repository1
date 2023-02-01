-- 创建数据库
create database if not exists hive02;
-- 选择数据库
use hive02;
-- 演示location改变数据表路径
create table stu
(
    id   int,
    name string,
    age  int
)
    location '/table';


-- 演示location改变数据库路径
create database if not exists cs1 location '/binzi';
-- 在cs1中创建表(表的路径跟着数据库走)
use cs1;
create table stu1
(
    id   int,
    name string,
    age  int
);


-- 其他分隔符(元素,map映射,多行)
/*文件hot_hero_skin_price.txt中记录了手游《王者荣耀》
热门英雄的相关皮肤价格信息，要求在Hive中建表映射成功该文件。
字段：id、name（英雄名称）、win_rate（胜率）、skin_price（皮肤及价格）
分析一下：前3个字段原生数据类型、最后一个字段复杂类型map。
    需要指定字段之间分隔符、集合元素之间分隔符、map kv之间分隔符。*/
-- 先选择数据库
use hive02;
-- 创建表
create table hot_hero_skin_price
(
    id         int,
    name       string,
    win_rate   int,
    skin_price map<string,int>
);
-- 手动去hdfs上传数据
-- 查询数据
select *
from hot_hero_skin_price;
-- 删除表(因为创建表的时候没有指定键值对映射分隔符,所以没有成功匹配数据)
drop table hot_hero_skin_price;

-- 演示所有map类型和分隔符(字段,元素,map映射,多行)
-- 创建表
create table hot_hero_skin_price
(
    id         int,
    name       string,
    win_rate   int,
    skin_price map<string,int>
) row format delimited
    fields terminated by ',' -- 字段分隔符
    collection items terminated by '-' -- 元素分隔符
    map keys terminated by ':' --键值对分隔符
    lines terminated by '\n';
--多行分隔符  默认
-- 手动去hdfs上传数据
-- 查询数据(效果:skin_price的结果就是python的字典)
select *
from hot_hero_skin_price;
select skin_price
from hot_hero_skin_price;
select map_keys(skin_price)
from hot_hero_skin_price;
select map_values(skin_price)
from hot_hero_skin_price;
select map_keys(skin_price)[0]
from hot_hero_skin_price;
select map_values(skin_price)[0]
from hot_hero_skin_price;
select size(skin_price)
from hot_hero_skin_price;


-- 演示所有array类型和分隔符(字段,元素,多行)
create table hot_hero_skin_price1
(
    id         int,
    name       string,
    win_rate   int,
    skin_price array<string>
) row format delimited
    fields terminated by ',' -- 字段分隔符
    collection items terminated by '-' -- 元素分隔符
    lines terminated by '\n';
--多行分隔符  默认
-- 手动去hdfs上传数据
-- 查询数据(效果:skin_price的结果就是python的字典)
select *
from hot_hero_skin_price1;
select skin_price
from hot_hero_skin_price1;
select skin_price[0]
from hot_hero_skin_price1;
select skin_price[1]
from hot_hero_skin_price1;
select skin_price[2]
from hot_hero_skin_price1;
select skin_price[3]
from hot_hero_skin_price1;
select skin_price[4]
from hot_hero_skin_price1;
select size(skin_price)
from hot_hero_skin_price1;


-- 演示所有struct类型和分隔符(字段,元素,多行)
create table hot_hero_skin_price2
(
    id         int,
    name       string,
    win_rate   int,
    skin_price struct<name:string,age:int>
) row format delimited
    fields terminated by ',' -- 字段分隔符
    collection items terminated by ':' -- 元素分隔符
    lines terminated by '\n';
--多行分隔符  默认
-- 手动去hdfs上传数据
-- 查询数据(效果:skin_price的结果就是python的字典)
select *
from hot_hero_skin_price2;
select skin_price.name
from hot_hero_skin_price2;
select skin_price.age
from hot_hero_skin_price2;


-- 演示分区表
/*分区表格式: create [externale] table 表名 (字段名 字段类型) PARTITIONED BY (分区字段 字段类型 ...);
注意: 分区字段名不能和当前表中字段名重复,因为分区字段会临时在当前表中作为一个普通字段使用(所有字段末尾)*/

-- 回顾不分区存储多个文件
create table tb_all_hero
(
    id           int,
    name         string,
    hp_max       int,
    mp_max       int,
    attack_max   int,
    defense_max  int,
    attack_range string,
    role_main    string,
    role_assist  string
) row format delimited
    fields terminated by '\t';
-- 上传数据后再查询数据
select *
from tb_all_hero;
-- 查询数据(全表扫描)
select *
from tb_all_hero
where role_main = 'archer';
select count(*)
from tb_all_hero
where role_main = 'archer';


-- 演示创建分区表(静态方式)
create table tb_all_hero_static
(
    id           int,
    name         string,
    hp_max       int,
    mp_max       int,
    attack_max   int,
    defense_max  int,
    attack_range string,
    role_main    string,
    role_assist  string
) PARTITIONED BY (role string)
    row format delimited
        fields terminated by '\t';

-- 还按照之前方式上传数据: 创建了分区表后,即使你把文件上传到了表目录中,那么数据依然是识别不到的,因为没有分区目录
select *
from tb_all_hero_static;
-- 查不到数据

-- 需要手动分区(静态): 先创建分区目录,并把指定文件移动到该目录中
load data inpath '/user/hive/warehouse/hive02.db/tb_all_hero_static/archer.txt' into table tb_all_hero_static partition (role = 'archer');
load data inpath '/user/hive/warehouse/hive02.db/tb_all_hero_static/assassin.txt' into table tb_all_hero_static partition (role = 'assassin');
load data inpath '/user/hive/warehouse/hive02.db/tb_all_hero_static/mage.txt' into table tb_all_hero_static partition (role = 'mage');
load data inpath '/user/hive/warehouse/hive02.db/tb_all_hero_static/support.txt' into table tb_all_hero_static partition (role = 'support');
load data inpath '/user/hive/warehouse/hive02.db/tb_all_hero_static/tank.txt' into table tb_all_hero_static partition (role = 'tank');
load data inpath '/user/hive/warehouse/hive02.db/tb_all_hero_static/warrior.txt' into table tb_all_hero_static partition (role = 'warrior');

-- 查询数据(已经有分区目录,所以能够查到数据)
select *
from tb_all_hero_static;

-- 查询数据
-- 注意即使你分区了,你查询的时候还是使用的原来方式查询,依然会全盘扫描
select *
from tb_all_hero_static
where role_main = 'archer';
-- 注意如果不想全盘扫描,条件需要使用分区字段名进行筛选
select *
from tb_all_hero_static
where role = 'archer';
select count(*)
from tb_all_hero_static
where role = 'archer';


-- 创建分区表(动态)
create table tb_all_hero_dynamic
(
    id           int,
    name         string,
    hp_max       int,
    mp_max       int,
    attack_max   int,
    defense_max  int,
    attack_range string,
    role_main    string,
    role_assist  string
) PARTITIONED BY (role string)
    row format delimited
        fields terminated by '\t';

-- 还按照之前方式上传数据: 创建了分区表后,即使你把文件上传到了表目录中,那么数据依然是识别不到的,因为没有分区目录
select *
from tb_all_hero_dynamic;
-- 查不到数据

-- 自动分区(动态)
-- insert into tb_all_hero_dynamic partition(role) select *,role_main from tb_all_hero;
insert into tb_all_hero_dynamic
select *, role_main
from tb_all_hero;

-- 查询数据
-- 注意即使你分区了,你查询的时候还是使用的原来方式查询,依然会全盘扫描
select *
from tb_all_hero_dynamic
where role_main = 'archer';
-- 注意如果不想全盘扫描,条件需要使用分区字段名进行筛选
select *
from tb_all_hero_dynamic
where role = 'archer';
select count(*)
from tb_all_hero_dynamic
where role = 'archer';

-- 分区练习(多重分区)
-- 演示不分区情况
-- 创建普通内部表,指定字段分隔符空格
create table tb_orders
(
    order_id int,
    name    string,
    price   float,
    count   int
)row format delimited
fields terminated by ' ';

-- 手动去hdfs上传文件
-- 查询数据
select * from tb_orders;

-- 演示多重分区
-- 创建内部分区表,指定字段分隔符空格
create table tb_orders_part
(
    order_id int,
    name    string,
    price   float,
    count   int
)partitioned by (year int,month int,day int)
row format delimited
fields terminated by ' ';

-- 手动去hdfs上传文件到/order目录下
-- 查询数据(没有分区目录查询不到)
select * from tb_orders_part;

-- 选择静态分区(静态和动态二选一)
load data inpath '/order/order415.txt' into table tb_orders_part partition (year=2022,month=4,day=15);
load data inpath '/order/order51.txt' into table tb_orders_part partition (year=2022,month=5,day=1);
load data inpath '/order/order52.txt' into table tb_orders_part partition (year=2022,month=5,day=2);

-- 查询数据(有分区目录能查到)
select * from tb_orders_part;

-- 需求1: 查询2022年销售额
select sum(price*count) from tb_orders_part where year=2022;
-- 需求2: 查询2022年5月份的销售额
select sum(price*count) from tb_orders_part where year=2022 and month = 5;
-- 需求3: 查询2022年5月1号的销售额
select sum(price*count) from tb_orders_part where year=2022 and month = 5 and day=1;


-- 演示分桶表
/*现有美国2021-1-28号，各个县county的新冠疫情累计案例信息，包括确诊病例和死亡病例，
字段含义如下：count_date（统计日期）,county（县）,state（州）,fips（县编码code）,
    cases（累计确诊病例）,deaths（累计死亡病例）。*/
-- 创建内部分桶表
create table tb_usa_covid(
    count_date date,
    county string,
    state string,
    fips string,
    cases int,
    deaths int
)
clustered by (state) into 3 buckets
row format delimited
fields terminated by ',';
-- 上传文件数据(和分区表不一样,分桶表虽然没有生成桶数据,但是可以匹配到文件数据)
select * from tb_usa_covid;


-- 演示静态分桶表
-- 创建内部分桶表
create table tb_usa_covid_static(
    count_date date,
    county string,
    state string,
    fips string,
    cases int,
    deaths int
)
clustered by (state) into 3 buckets
row format delimited
fields terminated by ',';


-- 上传文件数据(和分区表不一样,分桶表虽然一开始没有生成桶数据,但是可以匹配到文件数据)
select * from tb_usa_covid_static;
-- 手动分桶(静态)
load data inpath '/user/hive/warehouse/hive02.db/tb_usa_covid_static/us-covid19-counties.dat' into table tb_usa_covid_static;
-- 分桶后查询数据
-- 注意: 静态方式分桶后,原有文件依然存在,导致数据重复,建议手动删除
select * from tb_usa_covid_static;
select sum(deaths) from tb_usa_covid_static;


-- 演示动态分桶表
-- 创建内部分桶表
create table tb_usa_covid_dynamic(
    count_date date,
    county string,
    state string,
    fips string,
    cases int,
    deaths int
)
clustered by (state) into 3 buckets
row format delimited
fields terminated by ',';

-- 动态分桶(前提:有一个存储所有数据的表用于查询)
insert into tb_usa_covid_dynamic select * from tb_usa_covid;

-- 分桶后查询数据
-- 注意: 静态方式分桶后,原有文件依然存在,导致数据重复,建议手动删除
select * from tb_usa_covid_dynamic;
select sum(deaths) from tb_usa_covid_dynamic;
desc formatted tb_usa_covid_dynamic;


-- 演示默认存储方式和表属性
create table store_pro(id int,name string,age int)
stored as textfile
tblproperties ('cls'='12期','date'='2022-12-31');

-- 查看表信息
desc formatted store_pro;

-- 面试题:内部表和外部表的转换
-- 1.内部表转换成外部表
alter table store_pro set tblproperties ('EXTERNAL'='TRUE');
-- 查看表信息
desc formatted store_pro;
-- 2.外部表转换成内部表
alter table store_pro set tblproperties ('EXTERNAL'='FALSE');
-- 查看表信息
desc formatted store_pro;













