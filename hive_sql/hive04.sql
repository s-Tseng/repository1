
-- 一.演示各种排序
-- 创建数据库
create database hive04;
-- 使用数据库
use hive04;
-- 创建分桶表
create table tb_students
(
    id     int,
    name   string,
    gender string,
    age    int,
    class  string
)clustered by (name) sorted by (id) into 4 buckets
row format delimited
fields terminated by ',';

-- 创建基础表
-- 功能1 : 用于分桶表源数据
-- 功能2 : 用于cluster by,distribute by 和 sort by的演示
create table students
(
    id     int,
    name   string,
    gender string,
    age    int,
    class  string
)
row format delimited
fields terminated by ',';

-- 创建基础表后,需要你手动去hdfs上传students.txt文件
-- 查询是否已经有数据
select * from students;

-- 可以使用动态方式给分桶表插入数据并自动分桶
insert into tb_students select * from students;


-- 演示建表时分桶的sorted by 桶内排序: 1个桶对应1个reduce
-- 分桶并排序后查询数据,找规律
select * from tb_students;

-- 演示直接使用order by 全局排序: 底层只有1个reduce
select * from students order by id;

-- 演示直接使用cluster by : 分且排序   特点: 分和排都是同一个字段
set mapreduce.job.reduces;  -- 查看reduce要求
-- mapreduce.job.reduces=-1 :  根据实际情况来
set mapreduce.job.reduces = 4; -- 直接设置了reduces数量
select * from students cluster by id;

-- 演示直接使用distribute by 和 sort by  特点: 一个负责分,一个负责排
-- 此种方式和上述分桶后查询效果一样,
-- 那么它和分桶区别是分桶表需要建表提前指定桶数量会输出文件,此种方式手动设置reduce数量没有对应文件
select * from students distribute by name sort by id;


-- 二.演示联合查询
-- 1.准备工作
-- 创建数据表
create table tb_archer(
    id int comment 'ID',
    name string comment '英雄名称',
    hp_max int comment '最大生命',
    mp_max int comment '最大法力',
    attack_max int comment '最高物攻',
    defense_max int comment '最大物防',
    attack_range string comment '攻击范围',
    role_main string comment '主要定位',
    role_assist string comment '次要定位'
)
row format delimited  -- 使用默认的切割方式
fields terminated by '\t';   -- 按照 制表符 \t 进行切割
-- 使用like关键字复制表结构
create table tb_mage like tb_archer;
-- 手动去hdfs分别上传archer.txt,mage.txt
-- 查询数据
select * from tb_archer;
select * from tb_mage;

-- 2. union联合查询
-- 注意: union单独使用默认有去重操作
select * from tb_archer
union
select * from tb_mage;

-- union distinct: 去重联合查询
select * from tb_archer
union distinct
select * from tb_mage;

-- union all: 不去重(展示所有数据)联合查询
select * from tb_archer
union all
select * from tb_mage;


-- union all配合order by排序
select * from tb_archer
union all
select * from tb_mage
order by hp_max desc;

-- union 配合子查询: 注意子查询语句需要起别名
select id,name from (select * from tb_archer where mp_max=1770) ar

union
select id,name from (select * from tb_mage where mp_max=1988) ma;



-- cte公用表达式的with语句是一个临时结果集
-- 需求1: with语句改造子查询语句
with ar as (select * from tb_archer where mp_max=1770),
     ma as (select * from tb_mage where mp_max=1988)
select id,name from ar
union
select id,name from ma;

-- 需求2: 创建两个表,复制tb_archer的结构数据
/*方式1: 同时复制表结构和数据
create table copy_archer1 as select * from tb_archer;
create table copy_archer2 as select * from tb_archer;
 */
/*方式2: 先复制表结构,再单独插入数据
create table copy_archer1 like tb_archer;
create table copy_archer2 like tb_archer;
insert into copy_archer1 select * from tb_archer;
insert into copy_archer2 select * from tb_archer;*/

-- 方式3: cte语句优化方式2
create table copy_archer1 like tb_archer;
create table copy_archer2 like tb_archer;
-- 以下关键点是with先生成临时结果集,from 前置使用结果集
with ar as (select * from tb_archer)
from ar
insert into copy_archer1 select *
insert into copy_archer2 select * ;


-- 演示join连接查询

-- 准备工作
--table1: 员工表
CREATE TABLE employee(
   id int,
   name string,
   deg string,
   salary int,
   dept string
 ) row format delimited
fields terminated by ',';

--table2:员工住址信息表
CREATE TABLE employee_address (
    id int,
    hno string,
    street string,
    city string
) row format delimited
fields terminated by ',';

--table3:员工联系方式表
CREATE TABLE employee_connection (
    id int,
    phno string,
    email string
) row format delimited
fields terminated by ',';

-- 手动去hdfs分别上传3个数据文件
-- 查询数据
select * from employee;
select * from employee_address;
select * from employee_connection;

-- 演示多表关联
select * from employee e,employee_address ea,employee_connection ec where e.id=ea.id and e.id=ec.id;
select * from employee e join employee_address ea join employee_connection ec on e.id=ea.id and e.id=ec.id;


-- 演示inner join（内连接inner可省略）
-- 隐式内连接
select * from employee e,employee_address ea where e.id=ea.id;
-- 显示内连接(inner可以省略)
select * from employee e join employee_address ea on e.id=ea.id;
select * from employee e inner join employee_address ea on e.id=ea.id;

-- 演示left outer join（左外连接outer可省略）: 以左表为主,右表没有的null补上
select * from employee e left join employee_address ea on e.id=ea.id;
select * from employee e left outer join employee_address ea on e.id=ea.id;


-- right join（右连接outer可省略）:以右表为主,左表没有的null补上
select * from employee e right join employee_address ea on e.id=ea.id;
select * from employee e right outer join employee_address ea on e.id=ea.id;


-- full outer join（全外连接outer可省略）
select * from employee e full join employee_address ea on e.id=ea.id;
select * from employee e full outer join employee_address ea on e.id=ea.id;


-- left semi join（左半开连接）: 展示左表里和右表关联成功的
select * from employee e left semi join employee_address ea on e.id=ea.id;
select * from employee_address ea  left semi join employee e on e.id=ea.id;

-- 笛卡尔积
-- 隐式内连接不加where就是笛卡尔积
select * from employee,employee_address;
-- 显示内连接(inner可以省略)不加on条件就是笛卡尔积
select * from employee inner join employee_address;
-- cross join（交叉连接）不加on条件就是笛卡尔积
select * from employee cross join employee_address;
-- cross join ... on 条件: 加了条件和内连接一个道理
select * from employee e cross join employee_address ea on e.id=ea.id;


-- 演示运算符和内置函数
-- 查看所有的运算符和内置函数
show functions ;
-- 查看某个运算符或者函数的基本说明
desc function +;
desc function size;
-- 查看某个运算符或者函数的详细说明
desc function extended +;
desc function extended size;

-- hive函数分类标准
-- UDF:user definition functions普通函数 : 一进一出
-- UDAF:user definition aggregation functions聚合函数:多进一出
-- UDTF:user definition Table_generate functions表生成函数: 一进多出

-- 演示匹配关键字基本使用
-- • LIKE比较: LIKE
select '张三丰' like '张_';
select '张三丰' like '张__';
select '张三丰' like '张%';

-- • 正则表达式操作: RLIKE
-- • 正则表达式操作:: REGEXP
select '张三丰' rlike '^张.$';
select '张三丰' regexp '^张.$';

select '张三丰' rlike '^张..$';
select '张三丰' regexp '^张..$';

select '张三丰' rlike '^张.*$';
select '张三丰' regexp '^张.*$';

-- 算术运算符
-- •取整操作: div
select 10 div 3;
-- •取余操作: %
select 10 % 3;
/* 8421
1: 0001
2: 0010

*/
-- 0 代表false  1代表true
-- •位与操作: &  : 有false则false
select 1&2;  -- 结果: 0000
-- •位或操作: |  : 有true则true
select 1|2;  -- 结果: 0011
-- •位异或操作: ^ : 相同为false,不同为true
select 1^2;  -- 结果: 0011
-- •位取反操作: ~x  结果:  -(x+1)
select ~1 ;


-- 演示字符串函数
-- •字符串长度函数：length
select length('张三丰');
-- •字符串反转函数：reverse
select reverse('张三丰');

-- •字符串连接函数：concat   行转列函数
select concat('python','大数据');
-- •带分隔符字符串连接函数：concat_ws  行转列函数
select concat_ws('-','python','大数据');

-- •字符串截取函数：substr,substring
select substr('python',0,2);
select substr('python',3,5);
select substr('python',3);
-- •字符串转大写函数：upper,ucase
select upper('binzi');
-- •字符串转小写函数：lower,lcase
select lower('BINZI');
-- •去空格函数：trim
select trim('  binzi  ');
-- •左边去空格函数：ltrim
select ltrim('  binzi  ');
-- •右边去空格函数：rtrim
select rtrim('  binzi  ');
-- •正则表达式替换函数：regexp_replace
desc function extended regexp_replace;
SELECT regexp_replace('100-200', '(\\d+)', 666);
SELECT regexp_replace('你TMD哦', '([a-zA-Z]+)', '挺萌的');
-- •正则表达式解析函数：regexp_extract
desc function extended regexp_extract;
SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
--
-- •URL解析函数：parse_url
desc function extended parse_url;
SELECT parse_url('http://node1:8088?query=yarn', 'HOST');
SELECT parse_url('http://node1:8088?query=yarn', 'QUERY');

-- •json解析函数：get_json_object    处理json数据
desc function extended get_json_object;
select get_json_object('{"name":"斌子","age":18}', '$.name');
select get_json_object('{"name":"斌子","age":18}', '$.age');


-- •空格字符串函数：space
desc function extended space;
SELECT space(5);

-- •重复字符串函数：repeat
select repeat('斌子666',6);

-- •首字符ascii函数：ascii
select ascii('0');
select ascii('A');
select ascii('a');

-- •左补足函数：lpad
select lpad('a',5,'0');
-- •右补足函数：rpad
select rpad('a',5,'0');

-- •分割字符串函数: split
select split('binzi-666','-');

-- •集合查找函数: find_in_set  注意: 查第几个 不是索引
desc function extended find_in_set;
SELECT find_in_set('a','a,b,c,d');

-- 演示日期函数
-- •获取当前日期: current_date
select current_date();
-- •获取当前时间戳: current_timestamp
select current_timestamp();
-- •UNIX时间戳转日期函数: from_unixtime
select from_unixtime(0,'Y-M-d H:m:S');  -- 格林制时间 1970年
select from_unixtime(1672768440,'Y-M-d H:m:S');

-- •获取当前UNIX时间戳函数: unix_timestamp
select unix_timestamp();

-- •指定格式日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp('2023-1-3 17:54:00');

-- •抽取日期函数: to_date
select to_date('2023-1-3 17:54:00');
-- •日期转年函数: year
select year('2023-1-3 17:54:00');
-- •日期转月函数: month
select month('2023-1-3 17:54:00');
-- •日期转天函数: day
select day('2023-1-3 17:54:00');
-- •日期转小时函数: hour
select hour('2023-1-3 17:54:00');
-- •日期转分钟函数: minute
select minute('2023-1-3 17:54:00');
-- •日期转秒函数: second
select second('2023-1-3 17:54:00');

-- •日期转周函数: weekofyear
select weekofyear('2023-1-3 17:54:00');
select dayofweek('2023-1-3 17:54:00');

-- •日期比较函数: datediff
select datediff('2023-1-3','2022-10-25');

-- •日期增加函数: date_add
select date_add('2023-1-3',1);
-- •日期减少函数: date_sub
select date_sub('2023-1-3',1);

-- 数学函数
select round(19.363636363636363,2);
select ceil(19.363636363636363);
select floor(19.363636363636363);

-- 窗口函数
-- 求平均年龄
select avg(age) from students;
-- 现在select后的字段要么在groupby后出现要么在聚合函数内出现
select name,age,avg(age) from students; -- 此行报错
-- 窗口函数
select name,age,avg(age) over() from students;
-- 需求: 求每个人年龄和平均年龄差值
select name,
       age,
       avg(age) over(),
       age - avg(age) over()
from students;

--
select * from students;

-- 窗口函数之排序函数
select name,
       age,
       class,
       row_number() over (order by age),  -- 1234
       dense_rank() over (order by age),  -- 1223
       rank() over (order by age)         -- 1224
from students;


-- 窗口函数之分组
select name,
       age,
       class,
       row_number() over (partition by class order by age),  -- 1234
       dense_rank() over (partition by class order by age),  -- 1223
       rank() over (partition by class order by age)         -- 1224
from students;

