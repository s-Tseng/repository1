-- 一. hive数据库的增删改查
-- 创建数据库
-- 创建cs1数据库: if not exists: 判断库是否存在  location:更改库存储路径
create database if not exists cs1 location '/db_cs1';
-- 查看cs1基本信息
desc database cs1;
-- 创建cs2数据库: if not exists: 判断库是否存在  没有指定location默认路径是:/user/hive/warehouse/
create database if not exists cs2 ;
-- 查看c2基本信息
desc database cs2;
-- 创建数据库:  if not exists: 判断库是否存在   with dbproperties(属性名=属性值)
create database if not exists hive03 with dbproperties ('name'='hive03','time'='2023/1/1');

-- 查看数据库
-- 查看hive03基本信息(不展示属性信息)
desc database hive03;
-- 查看hive03整体信息(extended:展示属性信息)
desc database extended hive03;
-- 查看数据库建库信息
show create database cs1;
show create database cs2;
show create database hive03;

-- 删除数据库
-- drop database 数据库名: 只能删除空数据库
drop database cs1 ;
-- drop database 数据库名 cascade: 能把库及库中表删除
use cs2;
create table test(id int,name string);
drop database cs2 cascade ;

-- 修改数据库
-- 修改默认存储路径变为 hdfs根路径下db_hive03
alter database hive03 set location 'hdfs://node1.itcast.cn:8020/db_hive03';
-- 创建表测试是否存储到了新的路径下
use hive03;
create table test1_hive03(id int,name string);
desc formatted test2_hive03;
-- 再次修改把路径修改到默认路径下
alter database hive03 set location 'hdfs://node1.itcast.cn:8020/user/hive/warehouse/hive03.db';
-- 再次创建表测试是否存储到了新的路径下
create table test2_hive03(id int,name string);

-- 修改数据库属性
alter database hive03 set dbproperties ('time'='2023-1-1');
alter database hive03 set dbproperties ('cls'='12');

-- 查看修改后的属性
desc database extended hive03;


-- 二.数据表的操作
-- 1.创建数据表
-- 创建内部表
create table stu_inner(
    id int,
    name string,
    age int
)row format delimited
fields terminated by ',';
-- 创建外部表
create external table stu_outer(
    id int,
    name string,
    age int
)row format delimited
fields terminated by ',';

-- 2.查看表信息
-- 查看元数据基本信息
desc stu_inner;
desc stu_outer;
-- 查看元数据详细信息
desc extended stu_inner;
desc extended stu_outer;
-- 查看元数据格式化后的详细信息
desc formatted stu_inner;
desc formatted stu_outer;
-- 查看建表信息
show create table stu_inner;
show create table stu_outer;
-- 查看表的属性
show tblproperties stu_inner;
show tblproperties stu_outer;

-- 3.删除表
-- 先准备测试数据
create table stu_inner2(id int,name string,age int)row format delimited fields terminated by ',';
create external table stu_outer2(id int,name string,age int)row format delimited fields terminated by ',';
select * from stu_inner2; -- 先手动去hdfs上传stu.txt文件,查询确定关联数据
select * from stu_outer2; -- 先手动去hdfs上传stu.txt文件,查询确定关联数据

-- 删除内部表数据: truncate删除hdfs中业务数据,mysql中的表字段信息依然还在
truncate table stu_inner2;
desc formatted stu_inner2;
-- 删除外部表数据: truncat不能删除外部表数据,会报错: 不能删除非托管表数据
truncate table stu_outer2;
desc formatted stu_outer2;

-- 删除内部表: drop删除内部表,会把hdfs中业务数据删除,并且mysql中表字段等元数据也删除
drop table stu_inner2;
-- 删除外部表: drop删除外部表,只能删除mysql表中的元数据,hdfs中的业务数据依然存在
drop table stu_outer2;


-- 4.修改表信息
-- 修改表名
-- 演示修改内部表
alter table  stu_inner rename to student_inner;
select * from student_inner;
-- 修改内部表名,既修改了mysql中元数据,也修改了hdfs中表对应目录名
desc formatted student_inner;

-- 演示修改外部表名
alter table  stu_outer rename to student_outer;
select * from student_outer;
-- 修改外部表名,只修改了mysql中元数据表名(location路径没有变化),hdfs中表对应目录名没有变化
desc formatted student_outer;

-- 修改字段
-- 修改内部表字段
alter table student_inner change id stuid int;
alter table student_inner change age stuage string;
-- 注意: int可以转换成string,但是string不能转换为int
alter table student_inner change name stuname int; -- 此行报错
select * from student_inner;
-- replace columns (字段名 字段类型...): 比较暴力,直接替换
alter table student_inner replace columns (id int,name string);
-- 添加列
alter table student_inner add columns (age int);

-- 修改外部表字段
alter table student_outer change id stuid int;
alter table student_outer change age stuage string;
-- 注意: int可以转换成string,但是string不能转换为int
alter table student_outer change name stuname int; -- 此行报错
select * from student_outer;
-- replace columns (字段名 字段类型...): 比较暴力,直接替换
alter table student_outer replace columns (id int,name string);
-- 添加列
alter table student_outer add columns (age int);


-- 修改表存储方式: alter table 表名 set fileformat ORC;
alter table student_inner set fileformat ORC;
desc formatted student_inner;
select * from student_inner; -- 数据是文本文件的,不支持orc
drop table student_inner;

-- 修改数据表路径: alter table 表名 set location 'hdfs://node1.itcast.cn:8020/路径';
-- 注意: location路径修改后的需要自己手动创建
alter table student_outer set location 'hdfs://node1.itcast.cn:8020/tb_outer';
desc formatted student_outer;
select * from student_outer;


-- 修改数据表属性: alter table 表名 set tblproperties (属性名=属性值);
-- 注意: 如果想要把外部表变成内部表,EXTERNAL必须大写,如果你写的小写不报错,只是帮你额外创建了一个
alter table student_outer set tblproperties ('EXTERNAL'='false');
desc formatted student_outer;


-- 5.修改表分区操作
-- 注意: 如果要做分区操作,创建表的时候必须指定分区
alter table student_outer add partition (year=2022); -- 此行报错
drop table student_outer;
-- 准备工作
-- 创建内部分区表
-- 创建内部表
create table stu_inner(
    id int,
    name string,
    age int
)partitioned by (year int)
row format delimited
fields terminated by ',';

-- 创建外部表
create external table stu_outer(
    id int,
    name string,
    age int
)partitioned by (year int)
row format delimited
fields terminated by ',';

-- 手动上传文件(此时不会生成分区目录,所以查询不到内容)
select * from stu_inner; -- 查询不到数据
select * from stu_outer; -- 查询不到数据
-- 自动生成分区并把文件移动到对应分区目录中
load data inpath '/user/hive/warehouse/hive03.db/stu_inner/stu.txt' into table stu_inner partition (year=2023);
load data inpath '/user/hive/warehouse/hive03.db/stu_outer/stu.txt' into table stu_outer partition (year=2023);


-- 添加分区(并手动去hdfs中直接把文件上传到对应分区目录)
alter table stu_inner add partition (year=2022);
select * from stu_inner where year=2022; -- 查询到数据

alter table stu_outer add partition (year=2022);
select * from stu_outer where year=2022; -- 查询到数据

-- 删除分区
-- 元数据被删除,能够把内部表对应hdfs中分区目录及数据都删除
alter table stu_inner drop partition (year=2022);
select * from stu_inner where year=2022; -- 查询不到数据

-- 元数据被删除,外部表对应的hdfs中分区目录及数据不能被删除
alter table stu_outer drop partition (year=2022);
select * from stu_outer where year=2022; -- 查询不到数据

-- 查询分区
show partitions stu_inner;
show partitions stu_outer;
-- 修复分区(手动在hdfs创建分区目录或者原来外部表删除的分区都能被恢复)
msck repair table stu_inner;
msck repair table stu_outer;

-- 修改分区名
alter table stu_inner partition (year=2020) rename to partition (year=2030);
alter table stu_outer partition (year=2020) rename to partition (year=2030);


-- 演示添加多重分区
alter table stu_inner add partition (year=2022,month=1,day=2); --此行报错
-- 准备多重分区表
create table stu_inner_multi(
    id int,
    name string,
    age int
)partitioned by (year int,month int,day int)
row format delimited
fields terminated by ',';
-- 添加多重分区(直接把数据文件上传到指定分区目录内)
alter table stu_inner_multi add partition (year=2022,month=1,day=2);
alter table stu_inner_multi add partition (year=2023,month=1,day=2);
select * from stu_inner_multi; -- 查询数据
-- 删除多重分区(可以直接删除某年,对应里面的月日都删除了)
alter table stu_inner_multi drop partition (year=2022);
-- 查询多重分区
show partitions stu_inner_multi;
-- 先手动在hdfs创建分区目录,然后再回来修复
msck repair table stu_inner_multi;
-- 修改分区名
alter table stu_inner_multi partition (year=2023,month=1,day=2) rename to partition (year=2024,month=2,day=2);

-- -----------------------------------------------
-- 演示从本地虚拟机上传文件到hdfs
-- 准备工作
-- 创建最基础的表(默认分隔符是SOH/^A)
create table stu(
    id int,
    name string,
    age int
);
-- 查询数据
select * from stu; -- 无数据
-- 方式1: 使用原始hdfs命令上传
/* 应该在虚拟机中操作
[root@node1 /]# mkdir test
[root@node1 /]# cd test
[root@node1 test]# vim stu.txt
1,张三,18
2,李四,28
3,王五,38
[root@node1 test]# hdfs dfs -put '/test/stu.txt' /user/hive/warehouse/hive03.db/stu/
*/
select * from stu; -- 有数据但是匹配失败显示null

-- 方式2: load方式(hive官方推荐的方式)
load data local inpath '/test/stu_default.txt' overwrite into table stu;
select * from stu; -- 有数据





-- 创建内部分区表
-- 以下三种方式前提是要创建的表和已有的stu_inner表字段结构一样
-- 方式1: 复制stu_inner表改名创建表,手动上传数据
create table stu_dynamic(
    id int,
    name string,
    age int
)partitioned by (year int)
row format delimited
fields terminated by ',';
-- 方式2: create table 表名 like关键字复制stu_inner表结构,手动上传
create table stu_dynamic_part like stu_inner;
-- 方式3: create table 表名 as select方式先复制表结构然后把数据自动写入
create table stu_dynamic_partition as select * from stu_inner;

select * from stu_inner; -- id name age year

-- 动态分区方式给stu_dynamic插入数据并生成分区
-- 设置非严格模式
set hive.exec.dynamic.partition.mode=nonstrict;
-- 动态插入
insert into stu_dynamic partition(year) select * from stu_inner;
-- 查看当前分区模式
set hive.exec.dynamic.partition.mode;
-- 设置严格模式
set hive.exec.dynamic.partition.mode=strict;
insert into stu_dynamic select * from stu_inner;
select * from stu_dynamic;



-- 查询表数据导出到hdfs文件(分隔符默认是SOH)
insert overwrite directory '/666' select * from stu_dynamic;
-- 查询表数据导出到本地虚拟机文件(分隔符默认是SOH)
insert overwrite local directory '/666' select * from stu_dynamic;

-- hdfs命令下载
/*直接把对应文件下载到本地(分隔符和文件保持一致)
[root@node1 /]# hdfs dfs -get /user/hive/warehouse/hive03.db/stu_dynamic/year=2023/000000_0 /
*/



-- 创建内部分桶表
create table tb_usa_covid19_buckets(
    count_date date,
    county string,
    state string,
    fips int,
    cases int,
    deaths int
)
clustered by(state) into 5 buckets
row format delimited
fields terminated by ',';

-- 创建基础表(目的为了一会儿动态分桶作为数据源使用)
create table tb_usa_covid19(
    count_date date,
    county string,
    state string,
    fips int,
    cases int,
    deaths int
)row format delimited
fields terminated by ',';


-- 添加数据自动分桶
insert into tb_usa_covid19_buckets select * from tb_usa_covid19;

-- 基础练习
-- 需求1: 查询所有数据
select * from tb_usa_covid19_buckets;
-- 需求2: 查询所有州确诊人数和死亡人数
select state,county,cases,deaths from tb_usa_covid19_buckets;
-- 需求3: 查询纽约确诊人数和死亡人数
select state,county,cases,deaths from tb_usa_covid19_buckets where state='New York';
-- 需求4: 查询各州确诊人数和死亡人数
select state,cases,deaths from tb_usa_covid19_buckets group by state, county, cases, deaths;
-- 需求5: 查询各州确诊总人数,死亡总人数
select state,sum(cases),sum(deaths) from tb_usa_covid19_buckets group by state;
-- 需求6: 查询各州确诊总人数,死亡总人数,加别名
select state,sum(cases) qz,sum(deaths) sw from tb_usa_covid19_buckets group by state;
-- 需求7: 查询各州死亡总人数大于10000
select state,sum(deaths) sw from tb_usa_covid19_buckets group by state having sum(deaths)>10000;
-- 需求8: 查询各州死亡总人数大于10000,并且倒序排序
select state,sum(deaths) sw from tb_usa_covid19_buckets group by state having sum(deaths)>10000 order by sum(deaths) desc;
-- 需求9: 查询各州死亡总人数大于10000,并且获取死亡人数最多的3个州
select state,sum(deaths) sw from tb_usa_covid19_buckets group by state having sum(deaths)>10000 order by sum(deaths) desc limit 3;





