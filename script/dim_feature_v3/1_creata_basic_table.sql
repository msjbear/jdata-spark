create table if not exists dev.dev_temp_msj_risk_jdata_user(
	user_id	 BIGINT COMMENT '用户ID',
	age	 	 INT    COMMENT '年龄段',
	sex	 	 INT	COMMENT '性别',
	user_lv_cd	 INT	COMMENT '用户等级',
	user_reg_dt STRING	COMMENT '用户注册日期'
)row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES("separatorChar"=",","escapeChar"= "\\")STORED AS TEXTFILE;

load data local inpath '../data/JData_User.csv' into table dev.dev_temp_msj_risk_jdata_user;

create table if not exists dev.dev_temp_msj_risk_jdata_product(
	sku_id	BIGINT COMMENT '商品编号',
	attr1	INT COMMENT '属性1',
	attr2	INT COMMENT '属性2',
	attr3	INT COMMENT '属性3',
	cate	BIGINT COMMENT '品类ID',
	brand	BIGINT COMMENT '品牌ID'
)row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES("separatorChar"=",","escapeChar"= "\\")STORED AS TEXTFILE;

load data local inpath '../data/JData_Product.csv' into table dev.dev_temp_msj_risk_jdata_product;


create table if not exists dev.dev_temp_msj_risk_jdata_comment(
	dt	STRING   COMMENT '截止到时间',
	sku_id		BIGINT   COMMENT '商品编号',
	comment_num	INT   	 COMMENT '累计评论数分段',
	has_bad_comment	INT   	 COMMENT '是否有差评',
	bad_comment_rate	DOUBLE   COMMENT '差评率'
)row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES("separatorChar"=",","escapeChar"= "\\")STORED AS TEXTFILE;

load data local inpath '../data/JData_Comment.csv' into table dev.dev_temp_msj_risk_jdata_comment;

create table if not exists dev.dev_temp_msj_risk_jdata_action(
	user_id	BIGINT COMMENT '用户编号',
	sku_id	BIGINT COMMENT '商品编号',
	time	STRING COMMENT '行为时间',
	model_id BIGINT COMMENT '点击模块编号，如果是点击',
	type		INT COMMENT '1.浏览（指浏览商品详情页）；2.加入购物车；3.购物车删除；4.下单；5.关注；6.点击',
	cate	BIGINT COMMENT '品类ID',
	brand		BIGINT COMMENT '品牌ID'
)row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES("separatorChar"=",","escapeChar"= "\\")STORED AS TEXTFILE;

load data local inpath '../data/JData_Action_0301_0315.csv' into table dev.dev_temp_msj_risk_jdata_action;
load data local inpath '../data/JData_Action_0316_0331.csv' into table dev.dev_temp_msj_risk_jdata_action;
load data local inpath '../data/JData_Action_0401_0415.csv' into table dev.dev_temp_msj_risk_jdata_action;



