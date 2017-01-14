-- noinspection SqlDialectInspectionForFile -- noinspection SqlNoDataSourceInspectionForFile

ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;

set hive.execution.engine=tez;

ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-api-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-common-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-dag-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-examples-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-ext-service-tests-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-history-parser-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-javadoc-tools-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-job-analyzer-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-mapreduce-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-runtime-internals-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-runtime-library-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-tests-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-ui-0.8.4.war;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-ui2-0.8.4.war;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-yarn-timeline-history-0.8.4.jar;
ADD JAR /home/broy/temp/apache-tez-0.8.4-bin/tez-yarn-timeline-history-with-acls-0.8.4.jar;

select count(*) from broy_tracker_event_server_ts_json_bigint_gzip where dt = 20160701 and hr = 2016070110 group by app_id limit 10;


--------------

CREATE EXTERNAL TABLE `broy_tracker_event_server_ts_json_bigint_gzip`(
  `fileid` bigint COMMENT 'from deserializer',
  `version` bigint COMMENT 'from deserializer',
  `uuid` string COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `app_id` bigint COMMENT 'from deserializer',
  `message` struct<device_info:struct<id_info:struct<dpid:string,dpid_sha1:string,dpid_md5:string,did:string,did_sha1:string,did_md5:string,mac:string,mac_sha1:string,mac_md5:string,aid:string,adv_id:string,idfa:string,idfv:string,r1_daid:string,wpid:string,opt_out:boolean,mobile_network_code:bigint,bluetooth_status:boolean,production_live:boolean,locale:string,mobile_country_code:bigint,jailbroken:boolean>,os:string,os_version:string,os_language:string,ip_v4:string,ip_v6:string,make:string,model:string,timezone_offset:bigint,country:string>,application_id:string,application_version:string,application_user_id:string,identifier_for_vendor:string,device_type:string,carrier_name:string,carrier_country:string,conn_type:string,screen_resolution:string,viewport_size:string,installed_app_info:array<struct<label:string,package_name:string>>,source:string,version:string,application_name:string,application_content_category:string,bundle_name:string,paid:boolean,store_url:string,store_ref_url:string,user_agent:string,user_language:string,term:string,sdk_version:string,event:struct<event_name:string,transaction_id:string,timestamp:bigint,key_value:array<struct<key:string,string_value:string,long_value:bigint,double_value:double,bool_value:boolean,key_value:string>>,non_interaction:boolean,lat:bigint,lon:bigint,session_id:string,altitude:double,course:double,speed:double,login:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>>,event:struct<event_action:string,event_label:string,event_value:bigint>,screen_view:struct<content_description:string,document_title:string,document_location_url:string,document_host_name:string,document_path:string>,registration:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>,reg_email_sha1:string,reg_street_address_sha1:string,reg_phone_sha1:string,reg_city:string,reg_state:string,zip:string,reg_country:string>,transaction_item:struct<transaction_id:string,line_item:struct<product_id:string,product_name:string,quantity:bigint,unit_of_measure:string,msr_price:bigint,price_paid:bigint,currency:string,item_category:string>>,transaction:struct<transaction_id:string,store_id:string,store_name:string,cart_id:string,order_id:string,total_sale:bigint,currency:string,shipping_costs:bigint,transaction_tax:bigint>,cart_create:struct<cart_id:string>,cart_add:struct<cart_id:string,line_item:struct<product_id:string,product_name:string,quantity:bigint,unit_of_measure:string,msr_price:bigint,price_paid:bigint,currency:string,item_category:string>>,cart_remove:struct<cart_id:string,line_item:struct<product_id:string,product_name:string,quantity:bigint,unit_of_measure:string,msr_price:bigint,price_paid:bigint,currency:string,item_category:string>>,cart_delete:struct<cart_id:string>,facebook_connect:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>,permission:array<struct<name:string,granted:boolean>>>,twitter_connect:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>,permission:array<struct<name:string,granted:boolean>>>,session_end:struct<session_length:bigint,subsessions:array<struct<hr:bigint,subsession_length:bigint>>>,region:struct<id:bigint,name:string,region_sets:array<struct<id:bigint,name:string>>,type:string,tags:array<struct<value:string>>,lat:double,lng:double,radius:double,uuid:string,minor_value:double,major_value:double>,session_marker:struct<type:string,subsession_length:bigint,se_end_ts:bigint>>> COMMENT 'from deserializer',
  `tracking_id` string COMMENT 'from deserializer',
  `ip_address` string COMMENT 'from deserializer',
  `x_forwarded_for` string COMMENT 'from deserializer',
  `device_dt` bigint COMMENT 'from deserializer',
  `device_hr` bigint COMMENT 'from deserializer',
  `server_dt` bigint COMMENT 'from deserializer',
  `server_hr` bigint COMMENT 'from deserializer')
PARTITIONED BY (
  `dt` bigint,
  `hr` bigint)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'alter.mode'='false')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'


nohup hive -e "use broy; ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar; set hive.exec.dynamic.partition=true; set hive.exec.dynamic.partition.mode=nonstrict; INSERT INTO TABLE broy_tracker_event_server_ts_json_bigint_gzip PARTITION (dt,hr) select * from radiumone.tracker_event_server_ts where dt >= 20160701 and dt <= 20160731;" &
nohup hive -e "use broy; ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar; set hive.exec.dynamic.partition=true; set hive.exec.dynamic.partition.mode=nonstrict; INSERT INTO TABLE broy_tracker_event_server_ts_json_bigint_gzip PARTITION (dt,hr) select * from radiumone.tracker_event_server_ts where dt >= 20160601 and dt <= 20160630;" &
nohup hive -e "use broy; ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar; set hive.exec.dynamic.partition=true; set hive.exec.dynamic.partition.mode=nonstrict; INSERT INTO TABLE broy_tracker_event_server_ts_json_bigint_gzip PARTITION (dt,hr) select * from radiumone.tracker_event_server_ts where dt >= 20160501 and dt <= 20160531;" &
nohup hive -e "use broy; ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar; set hive.exec.dynamic.partition=true; set hive.exec.dynamic.partition.mode=nonstrict; INSERT INTO TABLE broy_tracker_event_server_ts_json_bigint_gzip PARTITION (dt,hr) select * from radiumone.tracker_event_server_ts where dt >= 20160410 and dt <= 20160430;" &
-- The next command errored out
nohup hive -e "use broy; ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar; set hive.exec.dynamic.partition=true; set hive.exec.dynamic.partition.mode=nonstrict; INSERT INTO TABLE broy_tracker_event_server_ts_json_bigint_gzip PARTITION (dt,hr) select * from radiumone.tracker_event_server_ts where dt >= 20160405 and dt <= 20160409;" &

--------------

INSERT OVERWRITE TABLE broy_tracker_event_server_ts_json PARTITION (dt,hr)
select fileid, version, uuid,  timestamp, app_id , to_json(message), tracking_id ,ip_address  ,x_forwarded_for ,device_dt, device_hr ,server_dt ,server_hr ,dt  ,hr from radiumone.tracker_event_server_ts
where hr between 2016071700 and 2016071704;

INSERT OVERWRITE TABLE broy_tracker_event_server_ts_json_bigint PARTITION (dt,hr)
select * from radiumone.tracker_event_server_ts
where hr between 2016071700 and 2016071704;

--------------


CREATE TABLE `broy_connect_applications` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`icon` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
`user_id` int(11) DEFAULT NULL,
`name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
`category_id` int(11) DEFAULT NULL,
`status_id` int(11) DEFAULT NULL,
`app_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
`backup_app_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
`keen_project_id` varchar(30) COLLATE utf8_unicode_ci DEFAULT NULL,
`keen_custom_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
`dashboard_config` text COLLATE utf8_unicode_ci,
`created_at` datetime DEFAULT NULL,
`updated_at` datetime DEFAULT NULL,
PRIMARY KEY (`id`)
)


insert into `broy_connect_applications` (`app_id`, `keen_project_id`, `keen_custom_key`) values ('06341684-DE04-4F35-9B26-70DAB5A710D0', '578543e09b4ad02d64eb3bf4', 'FEC72BB32D8C8ABE82D9B334422F74EE501F7422A8CB59F8B791B53008E00E38');



-------------------


-- For V1 of Connect project
CREATE EXTERNAL TABLE `broy_tracker_event_server_ts_json_gzip_v1`(
  `fileid` bigint COMMENT 'from deserializer',
  `version` bigint COMMENT 'from deserializer',
  `uuid` string COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `app_id` bigint COMMENT 'from deserializer',
  `message` struct<device_info:struct<id_info:struct<dpid:string,dpid_sha1:string,dpid_md5:string,did:string,did_sha1:string,did_md5:string,mac:string,mac_sha1:string,mac_md5:string,aid:string,adv_id:string,idfa:string,idfv:string,r1_daid:string,wpid:string,opt_out:boolean,mobile_network_code:bigint,bluetooth_status:boolean,production_live:boolean,locale:string,mobile_country_code:bigint,jailbroken:boolean>,os:string,os_version:string,os_language:string,ip_v4:string,ip_v6:string,make:string,model:string,timezone_offset:bigint,country:string>,application_id:string,application_version:string,application_user_id:string,identifier_for_vendor:string,device_type:string,carrier_name:string,carrier_country:string,conn_type:string,screen_resolution:string,viewport_size:string,installed_app_info:array<struct<label:string,package_name:string>>,source:string,version:string,application_name:string,application_content_category:string,bundle_name:string,paid:boolean,store_url:string,store_ref_url:string,user_agent:string,user_language:string,term:string,sdk_version:string,event:struct<event_name:string,transaction_id:string,timestamp:bigint,key_value:array<struct<key:string,string_value:string,long_value:bigint,double_value:double,bool_value:boolean,key_value:string>>,non_interaction:boolean,lat:bigint,lon:bigint,session_id:string,altitude:double,course:double,speed:double,login:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>>,event:struct<event_action:string,event_label:string,event_value:bigint>,screen_view:struct<content_description:string,document_title:string,document_location_url:string,document_host_name:string,document_path:string>,registration:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>,reg_email_sha1:string,reg_street_address_sha1:string,reg_phone_sha1:string,reg_city:string,reg_state:string,zip:string,reg_country:string>,transaction_item:struct<transaction_id:string,line_item:struct<product_id:string,product_name:string,quantity:bigint,unit_of_measure:string,msr_price:bigint,price_paid:bigint,currency:string,item_category:string>>,transaction:struct<transaction_id:string,store_id:string,store_name:string,cart_id:string,order_id:string,total_sale:bigint,currency:string,shipping_costs:bigint,transaction_tax:bigint>,cart_create:struct<cart_id:string>,cart_add:struct<cart_id:string,line_item:struct<product_id:string,product_name:string,quantity:bigint,unit_of_measure:string,msr_price:bigint,price_paid:bigint,currency:string,item_category:string>>,cart_remove:struct<cart_id:string,line_item:struct<product_id:string,product_name:string,quantity:bigint,unit_of_measure:string,msr_price:bigint,price_paid:bigint,currency:string,item_category:string>>,cart_delete:struct<cart_id:string>,facebook_connect:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>,permission:array<struct<name:string,granted:boolean>>>,twitter_connect:struct<user_data:struct<user_id:string,user_id_sha1:string,user_name_sha1:string>,permission:array<struct<name:string,granted:boolean>>>,session_end:struct<session_length:bigint,subsessions:array<struct<hr:bigint,subsession_length:bigint>>>,region:struct<id:bigint,name:string,region_sets:array<struct<id:bigint,name:string>>,type:string,tags:array<struct<value:string>>,lat:double,lng:double,radius:double,uuid:string,minor_value:double,major_value:double>,session_marker:struct<type:string,subsession_length:bigint,se_end_ts:bigint>>> COMMENT 'from deserializer',
  `tracking_id` string COMMENT 'from deserializer',
  `ip_address` string COMMENT 'from deserializer',
  `x_forwarded_for` string COMMENT 'from deserializer',
  `device_dt` bigint COMMENT 'from deserializer',
  `device_hr` bigint COMMENT 'from deserializer',
  `server_dt` bigint COMMENT 'from deserializer',
  `server_hr` bigint COMMENT 'from deserializer')
PARTITIONED BY (
  `dt` int,
  `hr` int)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'alter.mode'='false')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'


-- Old type partition
INSERT OVERWRITE TABLE broy_tracker_event_server_ts_json_gzip_v1 PARTITION (dt,hr)
select * from radiumone.tracker_event_server_ts
where dt between 20160615 and 20160715;

-- Partition by keen payload records list size of 5000
INSERT OVERWRITE TABLE broy_tracker_event_server_ts_json_gzip_v1 PARTITION (dt,hr)
select * from radiumone.tracker_event_server_ts
where dt between 20160615 and 20160715;


------------------------------------------------------

[broy@jobserver2 keen]$ export PYTHONPATH="/home/broy/connect/r1-dw-connect-app"

[broy@jobserver2 keen]$ nohup /opt/python2.7/bin/python /home/broy/connect/r1-dw-connect-app/integration/keen/keen.py &

------------------------------------------------------




CREATE TABLE `data_metrics_source` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start_date` int(11) DEFAULT NULL,
  `end_date` int(11) DEFAULT NULL,
  `keen_project_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `app_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `rowcount` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `process_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `entity_name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
)

CREATE TABLE `data_metrics_sink` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start_date` int(11) DEFAULT NULL,
  `end_date` int(11) DEFAULT NULL,
  `keen_project_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `app_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `rowcount` int(11) DEFAULT NULL,
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `process_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `entity_name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
)

------------------------------------------------------

CREATE TABLE `connect_rt_file_process` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dt` int(11) DEFAULT NULL,
  `hr` int(11) DEFAULT NULL,
  `filename` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `process_name` varchar(4000) DEFAULT NULL,
  `entity_name` varchar(100) DEFAULT NULL,
  `rowcount` int(11) DEFAULT NULL,
  `processing_time` int(11) DEFAULT '-1',
  `date_new` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_in_process` timestamp NULL DEFAULT NULL,
  `date_processed` timestamp NULL DEFAULT NULL,
  `date_error` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UNIQUE_FILE` (`dt`,`hr`,`filename`,`entity_name`)
);

--------------------------------------------------------

hadoop jar parquet-tools-1.6.0.jar cat -j hdfs://server:8020/data/radiumone/tracker-event-server-ts/dt=20160831/hr=2016083112/radium1_tracker-event-server-ts_220639571_2016083112_0.parquet
-- Some fields are encoded in BASE64 format by parquet tools



mysql -ureadonly -hec2-54-175-102-162.compute-1.amazonaws.com -pWBmPY4rzNpR5 connect_production_v3
