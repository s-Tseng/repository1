-- 注释是ctrl+/
-- 一.数据库操作
-- 1.创建数据库
create database cs1;
create database if not exists cs1;
create database if not exists cs2;
create database if not exists cs3;

-- 2.删除数据库
-- 注意: drop database 库名 方式如果库中有表无法删除
drop database cs2;
-- 如果给cs3添加了表,如何一并删除?
-- 创建表
use cs3;
create table stu(id int ,name varchar(20));
-- 演示原始方式删除,报错
drop database cs3;
-- 原始方式最后加cascade,能够一并把库中表删除
drop database cs3 cascade;

-- 3.切换数据
use cs1;

-- 4.查询数据库
-- 查询所有数据库
show databases;
-- 查询当前数据库
select current_database();
-- 查询数据库信息
desc database cs1;


-- 二.库中表的操作
-- 注意: 先有库,并且切换数据库
create database if not exists hive01;
use hive01;
-- 1.创建表
create table stu(id int,name string);
create table if not exists stu(id int,name string);
create table if not exists stu1(id int,name string);
create table if not exists stu2(id int,name string);

-- 2.删除表
drop table stu2;

-- 3.修改表
alter table stu1 replace columns (userid int,username string);
alter table stu1 change userid id int;
alter table stu1 replace columns (id int);


-- 4.查看表
-- 查看所有表
show tables;
-- 查看表信息
desc stu1;
desc stu;
-- formatted关键能够查看详细的表信息
desc formatted stu;

-- 三.表中数据的操作(了解)
-- 1.插入数据
-- 插入1条数据
insert into stu (id,name) values (1,'张三');
insert into stu values (2,'李四');
-- 插入多条数据
insert into stu values (3,'王五'),(4,'赵六');

-- 2.删除数据(不支持delete删除)
truncate table stu;

-- 3.不支持修改数据

-- 4.查询数据
select * from stu;
select * from stu where id=1;
select * from stu where name='张三';



-- 内部表和外部表
-- 1.演示内部表
-- 选择数据库
use hive01;
-- 创建内部表(默认MANAGED_TABLE)
create table test_inner(id int , name string , age int);
-- 查看详细信息
desc formatted test_inner;
-- 添加数据
-- 注意1: 文件需要放到hdfs的/user/hive/warehouse/hive01.db/test_inner/
-- 注意2: 文件分隔符默认是SOH(0001),如果分割符不对,就无法识别
-- 注意3: hdfs...test_inner目录下可以存放多个文件,而且只要分隔符符号要求都会识别
-- 查询数据
select * from test_inner;
-- 删除内部表(mysql中的元数据和hdfs中的业务数据都被删除了)
drop table test_inner;



-- 2.演示外部表(EXTERNAL_TABLE)
-- 如果想要创建外部表需要在table前加external
create external table test_outer(id int , name string , age int);
-- 查看详细信息
desc formatted test_outer;
-- 添加数据
-- 注意1: 文件需要放到hdfs的/user/hive/warehouse/hive01.db/test_outer/
-- 注意2: 文件分隔符默认是SOH(0001),如果分割符不对,就无法识别
-- 注意3: hdfs...test_inner目录下可以存放多个文件,而且只要分隔符符号要求都会识别
-- 查询数据
select * from test_outer;
-- 删除外部表(只删除了mysql中的元数据,hdfs中业务数据保留下来了)
drop table test_outer;

-- 面试题: 内部表和外部表的区别?
-- 类型不同: 内部表类型是: MANAGED_TABLE  外部表类型是: EXTERNAL_TABLE
-- 删除效果不同: 删除内部表hdfs中的文件也会被删除  删除外部表hdfs中的文件不会被删除

-- 面试题: 内部表和外部表的应用场景?
-- 如果你对hdfs的文件有绝对控制权(文件只有当前表使用),就选择内部表,反之选择外部表

-- 3.字段分隔符(默认是SOH/0001)
-- 因为默认的符号不好输入,如果每次用默认太繁琐
-- 手动设置分隔符: row format delimited fields terminated by 分隔符
-- 创建内部表(默认MANAGED_TABLE)
create table student(id int , name string , age int)
row format delimited fields terminated by ',';
-- 查看详细信息
desc formatted student;
-- 添加数据
-- 注意1: 文件需要放到hdfs的/user/hive/warehouse/hive01.db/test_inner/
-- 注意2: 文件分隔符默认是SOH(0001),如果分割符不对,就无法识别
-- 注意3: hdfs...test_inner目录下可以存放多个文件,而且只要分隔符符号要求都会识别
-- 查询数据
select * from student;
-- 删除内部表(mysql中的元数据和hdfs中的业务数据都被删除了)
drop table student;


-- 4.字段默认分隔符练习
/*文件team_ace_player.txt中记录了手游《王者荣耀》主要战队内最受欢迎的王牌选手信息，
字段：id、team_name（战队名称）、ace_player_name（王牌选手名字）
要求在Hive中建表映射成功该文件。*/
-- 创建表
create table team_ace_player(id int,team_name string,ace_player_name string);
-- 添加数据(自己去hdfs页面手动添加)
-- 查询数据
select * from team_ace_player;


-- 5.字段分隔符自定义练习
/*文件archer.txt中记录了手游《王者荣耀》射手的相关信息，
其中字段之间分隔符为制表符\t,要求在Hive中建表映射成功该文件。
字段含义：id、name（英雄名称）、hp_max（最大生命）、mp_max（最大法力）、
attack_max（最高物攻）、defense_max（最大物防）、attack_range（攻击范围）、
role_main（主要定位）、role_assist（次要定位）。*/
-- 创建表
create table archer(
    id int,
    name string,
    hp_max int,
    mp_max int,
    attack_max int,
    defense_max int,
    attack_range string,
    role_main string,
    role_assist string
)row format delimited fields terminated by '\t';
-- 添加数据(自己去hdfs页面添加)
-- 查询数据
select * from archer;