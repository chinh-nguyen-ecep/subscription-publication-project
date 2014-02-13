---- create customers 

INSERT INTO control.spctl_pub_customer(customer_name,customer_host_name,customer_destination_folder,folder_content_transfer_script,transfer_script_name) 
VALUES ('s3-amazon','s3-amazon','ecep/subscription_publication_app','/home/postgres/bin/subscription_publication','transferToS3.pl');

INSERT INTO control.spctl_pub_customer(customer_name,customer_desc,folder_content_transfer_script,transfer_script_name) 
VALUES ('ecepvn','Ecep group from VietNam','/home/postgres/bin/subscription_publication','publicToEmail.pl');

INSERT INTO control.spctl_customer_contact(customer_email,customer_phone_number,customer_key)
VALUES ('chinh.nguyen@ecepvn.org','+84982777098',2);
INSERT INTO control.spctl_customer_contact(customer_email,customer_phone_number,customer_key)
VALUES ('tho.hoang@ecepvn.org','',2);
INSERT INTO control.spctl_customer_contact(customer_email,customer_phone_number,customer_key)
VALUES ('song.nguyen@ecepvn.org','',2);

---- create one subscription for customer S3
INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Daily Ad Serving Stats Aggregate','','DAILY',TRUE,1);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (1);

INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Monthly Aggregate','','MONTHLY',TRUE,1);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (2);



INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Daily Adcel reports','','DAILY',FALSE,2);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (2);

--- create export module
INSERT INTO control.spctl_export_module(module_desc,bin_dir,script_name,export_dir,export_file_name_format) VALUES ('This script export daily data from table','/home/postgres/bin/subscription_publication','export_daily_agg_data.pl','/data2/outgoing/daily_aggregate','export_mode.table_name.export_date.md5.timecode.csv');

INSERT INTO control.spctl_export_module(module_desc,bin_dir,script_name,export_dir,export_file_name_format) VALUES ('This script used to generate jasper report','/home/postgres/bin/subscription_publication','export_jasper_report.pl','/data2/outgoing/jasper_reports','reportName.date.md5.format');

--- create data subject
INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Daily Adcel aggregate','This subject content Adcel aggregate tables');
INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Daily ThirdParty aggregate','This subject content ThirdParty aggregate tables');
INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Daily ADM aggregate','This subject content ADM aggregate tables');
INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Daily EventTracker aggregate','This subject content EventTracker aggregate tables');
INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Daily Adsops aggregate','This subject content EventTracker aggregate tables');

INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Monthly aggregate data','This subject content all monthly aggregate tables');
INSERT INTO control.spctl_data_subject(data_subject_name,data_subject_desc) VALUES ('Jasper reports','This subject content all jasper reports');
--insert data file config

SELECT 'INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,data_subject_id,export_module_id,data_source_table_id) VALUES ('''||table_name||''',''csv'','''','''',6,1,'||data_source_table_id||');' 
FROM control.spctl_data_source_tables 
WHERE table_type='MLA'
AND date_up_to_date IS NOT NULL
ORDER BY table_name
;
--Jasper report
INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,df_attribute,job_id,export_module_id,data_source_table_id)
VALUES ('Daily Adcel Statistic Summary','pdf','Adcel report','/data/outgoing/jasper_reports/sources/Adcel reports/jaspers/daily_ad_serving_statistics_summary/daily_ad_serving_statistics_summary.jrxml','roll_back_date=0 eastern_date_sk={v_eastern_date_sk}',2,2,1);
INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,df_attribute,job_id,export_module_id,data_source_table_id)
VALUES ('Daily Adcel Statistic Summary','csv','Adcel report','/data/outgoing/jasper_reports/sources/Adcel reports/jaspers/daily_ad_serving_statistics_summary/daily_ad_serving_statistics_summary.jrxml','roll_back_date=0 eastern_date_sk={v_eastern_date_sk}',2,2,1);
INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,df_attribute,job_id,export_module_id,data_source_table_id)
VALUES ('Daily Adcel Statistic Summary','xls','Adcel report','/data/outgoing/jasper_reports/sources/Adcel reports/jaspers/daily_ad_serving_statistics_summary/daily_ad_serving_statistics_summary.jrxml','roll_back_date=0 eastern_date_sk={v_eastern_date_sk}',2,2,1);
-------

---- create article map data file config to subscription
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (1,1);
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (1,2);
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (1,3);

INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (2,4);
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (2,5);
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (2,6);

UPDATE control.spctl_data_source_tables a
SET date_up_to_date=b.full_date
,week_up_to_date=b.year_week_monday
,month_sine_2005_up_to_date=b.month_since_2005
,month_up_to_date=b.calendar_year_month
FROM refer.date_dim b
WHERE b.full_date='2013-06-09';
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1);

---------------------
INSERT INTO control.data_file_configuration(
            data_file_config_name, data_file_config_description, 
            data_file_target_table, data_file_type, 
            unzip, transform_on_import, 
            import_dir, success_dir, 
            error_dir, data_file_column_list, 
            transform_script_name, data_file_process_function, 
            data_file_process_wrapper_name, process_precedence)
    VALUES ('Aggregate Data File','Aggregate Data File'
	,'','D'
	,TRUE,FALSE
	,'/home/file_xfer/logs/aggregate_data/','/home/file_xfer/logs/aggregate_data/done/'
	,'/home/file_xfer/logs/aggregate_data/error/',''
	,'aggregate_data_import_script.pl',''
	,'fn_refresh_import_aggregate_data',0
    );

INSERT INTO control.data_file(file_name,
			server_name,
			file_timestamp,
			data_file_config_id,
			file_status,
			dt_file_queued)
VALUES ('daily.adstraffic.daily_ad_serving_stats.2013-06-20.147001f506056e8eea306bf096740454.20130621T23333100000.csv.zip',
	's3',
	'2013-06-20'::timestamp without time zone,
	2,
	'ER',
	now()::timestamp without time zone

);

---Query select 
SELECT 
a.subject_name
,b.job_name
,b.job_id
,d.type_name
,c.df_config_name
,c.df_config_format
,c.df_source_table
,e.customer_host_name
,f.subscription_name
,f.subscription_key
,g.customer_article_key
,d.export_dir
,d.export_script_name
,c.export_file_name
,h.next_fire_time
FROM 
control.spctl_subject a
,control.spctl_job b
,control.spctl_data_file_config c
,control.spctl_data_source_type d
,control.spctl_pub_customer e
,control.spctl_pub_customer_subscription f
,control.spctl_pub_customer_article g
,control.spctl_subscription_publication_checkpoint h
WHERE 
a.subject_status='ACTIVE' AND
b.job_status ='ACTIVE' AND
c.df_status='ACTIVE' AND
e.customer_status='ACTIVE' AND
f.subscription_status='ACTIVE' AND
a.subject_id=b.subject_id AND 
c.job_id=b.job_id AND 
d.data_source_type_id=c.data_source_type_id AND
e.customer_key=f.customer_key AND
g.subscription_key=f.subscription_key AND
g.df_config_id=c.df_config_id AND
h.subscription_key=f.subscription_key
AND f.subscription_key=1;

------------------------------------------------
UPDATE control.spctl_subscription_publication_process_concurrent_trans 
SET status='WT',export_file_name=null,md5_code=null,file_size=0,is_exported=FALSE,dt_starttime=now(),dt_lastchange=now(),process_id=0,error_message=null;

SELECT * FROM control.spctl_subscription_publication_process_concurrent_trans;
SELECT * FROM control.fn_spctl_refresh_daily_export_data_file();

UPDATE control.spctl_data_source_tables
SET date_up_to_date='2013-06-06';
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1);

perl main.pl daily dw3 dw10:analyticsdb adstraffic.daily_ad_serving_stats 2013-06-09 test
perl main.pl daily dw3 dw10:analyticsdb adstraffic.daily_ad_serving_stats_by_device 2013-06-09 test
perl main.pl daily dw3 dw10:analyticsdb adstraffic.daily_ad_serving_stats_by_content_category 2013-06-09 test

-------------------------------------------
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1);
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1,'date=2013-07-14',true);
SELECT * FROM control.fn_spctl_insert_subscription_to_process(2,'calendar_year_month=2013-Aug',true);
SELECT * FROM control.spctl_subscription_publication_process_concurrent_trans WHERE publication_process_id=1141;
SELECT * FROM control.spctl_subscription_publication_process WHERE publication_process_id=1141;

UPDATE control.spctl_subscription_publication_process SET process_status='WT';
UPDATE control.spctl_subscription_publication_process_concurrent_trans SET status='ER' WHERE publication_process_id=1;
UPDATE control.spctl_subscription_publication_process SET process_status='TR' WHERE publication_process_id=1;
SELECT * FROM control.spctl_subscription_publication_process WHERE process_status<>'SU' AND process_status<>'WT';
SELECT * FROM control.spctl_data_source_tables
----View job and data_file_config
SELECT * FROM control.spctl_job;
SELECT b.data_subject_name,a.df_config_name,a.df_config_id FROM control.spctl_data_file_config a,control.spctl_data_subject b WHERE a.data_subject_id=b.data_subject_id AND b.data_subject_id=6;
-----------
-- View customers

SELECT * FROM control.spctl_pub_customer;
SELECT a.* FROM control.spctl_customer_contact a,control.spctl_pub_customer b WHERE a.customer_key=b.customer_key AND b.customer_name LIKE '%ecep%';
-- Data subject
SELECT * FROM control.spctl_data_subject;
SELECT * FROM control.spctl_export_module;
SELECT * FROM control.spctl_data_source_tables WHERE table_name LIKE '%adstraffic.daily_ad_serving_stats%';
SELECT table_name,date_up_to_date,week_up_to_date,month_sine_2005_up_to_date,month_up_to_date FROM control.spctl_data_source_tables WHERE table_type='DLA' ORDER BY table_name;
-- Data file config
INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,df_attribute,data_subject_id,export_module_id,data_source_table_id)
VALUES ('Daily Adcel Statistic Summary','xls','Adcel report','/home/postgres/bin/subscription_publication/jasperReportTemplates/Adcel reports/jaspers/daily_ad_serving_statistics_summary/daily_ad_serving_statistics_summary.jrxml','roll_back_date=0 eastern_date_sk={v_eastern_date_sk}',7,2,415);

INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,df_attribute,data_subject_id,export_module_id,data_source_table_id)
VALUES ('Daily Adcel Statistic Summary csv','csv','Adcel report','/home/postgres/bin/subscription_publication/jasperReportTemplates/Adcel reports/jaspers/daily_ad_serving_statistics_summary/daily_ad_serving_statistics_summary.jrxml','roll_back_date=0 eastern_date_sk={v_eastern_date_sk}',7,2,415);

INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,df_attribute,data_subject_id,export_module_id,data_source_table_id)
VALUES ('Daily Adcel Statistic Summary pdf','pdf','Adcel report','/home/postgres/bin/subscription_publication/jasperReportTemplates/Adcel reports/jaspers/daily_ad_serving_statistics_summary/daily_ad_serving_statistics_summary.jrxml','roll_back_date=0 eastern_date_sk={v_eastern_date_sk}',7,2,415);

SELECT a.* FROM control.spctl_data_file_config a,control.spctl_data_subject b WHERE a.data_subject_id=b.data_subject_id AND b.data_subject_name='Jasper reports';

-- Subscriptions

INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Daily Adcel Statistic Reports','','DAILY',TRUE,2);
SELECT * FROM control.fn_spctl_connect_subscription_to_data_file('ecepvn','Daily Adcel Statistic Reports','Daily Adcel Statistic Summary,Daily Adcel Statistic Summary csv,Daily Adcel Statistic Summary pdf');

SELECT a.* FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer b WHERE a.customer_key=b.customer_key AND b.customer_name LIKE '%ecep%';
------------------
-- View subacription and data file

SELECT a.* FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer b WHERE a.customer_key=b.customer_key AND b.customer_name LIKE 'ecepvn';

SELECT * FROM control.fn_spctl_connect_subscription_to_data_file('s3-amazon','Monthly Aggregate','adsops.monthly_agg_delivery_adnetwork_publisher_v3,adsops.monthly_agg_delivery_publisher_property_v3');

SELECT * FROM control.spctl_pub_customer_subscription;

UPDATE control.spctl_pub_customer_subscription SET zip_before_transfer=true;
SELECT a.subscription_name,c.df_config_name FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer_article b,control.spctl_data_file_config c
WHERE a.subscription_key=b.subscription_key AND b.df_config_id=c.df_config_id AND a.subscription_key=2;

SELECT a.subscription_name,a.subscription_key,c.df_config_name,d.table_name FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer_article b,control.spctl_data_file_config c,control.spctl_data_source_tables d WHERE a.subscription_key=b.subscription_key AND b.df_config_id=c.df_config_id AND d.data_source_table_id=c.data_source_table_id AND b.articel_status = 'ACTIVE' ORDER BY a.subscription_name;

SELECT * FROM control.fn_spctl_insert_subscription_to_process(26,'',true);


--------
-- INACTIVE article
UPDATE control.spctl_pub_customer_article SET articel_status='INACTIVE' WHERE customer_article_key IN (14,12,8);
----
UPDATE control.spctl_subscription_publication_process_concurrent_trans 
SET status='WT',export_file_name=null,md5_code=null,file_size=0,is_exported=FALSE,dt_starttime=now(),dt_lastchange=now(),process_id=0,error_message=null;

UPDATE control.spctl_subscription_publication_process_concurrent_trans 
SET status='WTP';
-------------------------------------
--- test process transfer
SELECT a.subscription_name,a.subscription_key,c.df_config_name,b.articel_status FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer_article b,control.spctl_data_file_config c
WHERE a.subscription_key=b.subscription_key AND b.df_config_id=c.df_config_id ORDER BY b.subscription_key

SELECT * FROM control.fn_spctl_insert_subscription_to_process(1,'start_date=2013-07-01&end_date=2013-07-10',true);
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1,'date=2013-06-27',true);

SELECT * FROM control.fn_spctl_insert_subscription_to_process(2,'calendar_year_month=2013-Apr',true);


SELECT a.* FROM control.spctl_subscription_publication_process a WHERE a.create_date::date='2013-08-20' ORDER BY a.publication_process_id;

SELECT a.* FROM control.spctl_subscription_publication_process_concurrent_trans a,control.spctl_subscription_publication_process b 
WHERE a.publication_process_id=b.publication_process_id AND b.create_date::date='2013-08-20' AND export_file_name LIKE '%filled%' ORDER BY b.publication_process_id,a.customer_article_key;

SELECT a.* FROM control.spctl_subscription_publication_process_concurrent_trans a,control.spctl_subscription_publication_process b 
WHERE a.publication_process_id=b.publication_process_id AND status <> 'SU' ORDER BY b.publication_process_id,a.customer_article_key;

----------------------------------- import config
INSERT INTO control.data_file_configuration(
            data_file_config_id,data_file_config_name, data_file_config_description, 
            data_file_target_table, data_file_type, unzip, transform_on_import, 
            import_dir, success_dir, error_dir, data_file_column_list, data_file_load_options, 
            data_file_mask, transform_script_name, data_file_process_function, 
            data_file_process_wrapper_name, process_precedence)
    VALUES (1000,'Aggregate Data File','Aggregate Data File','','D',TRUE,FALSE,'/home/file_xfer/logs/warehouse_aggregate_files/','/home/file_xfer/logs/warehouse_aggregate_files/done/','/home/file_xfer/logs/warehouse_aggregate_files/error/','','','','aggregate_data_import_script_warehouse.pl','','fn_refresh_import_aggregate_data',0);

SELECT * FROM control.data_file_configuration WHERE data_file_config_name='Aggregate Data File';
ALTER TABLE control.data_file ALTER COLUMN file_name TYPE character varying(255);

usermod -a -G file_xfer  dw_job_agent
usermod -a -G dwadmin  dw_job_agent

INSERT INTO control.data_file(file_name,server_name,file_timestamp,data_file_config_id,file_status,dt_file_queued)
VALUES ('daily.adstraffic.daily_ad_serving_stats.2013-07-01.c31602e39770f609a0316e763f93aa29.20130702T08513200000.csv.zip','dw10','2013-07-01'::timestamp without time zone,1000,'ER',now()::timestamp without time zone);
INSERT INTO control.data_file(file_name,server_name,file_timestamp,data_file_config_id,file_status,dt_file_queued)
VALUES ('daily.adstraffic.daily_ad_serving_stats.2013-07-02.a5600c191f30912ef1e2709a30d5ab8b.20130703T02293300000.csv.zip','dw10','2013-07-02'::timestamp without time zone,1000,'ER',now()::timestamp without time zone);

SELECT * FROM control.data_file WHERE data_file_config_id=1000 ORDER BY data_file_id desc;
SELECT COUNT(1) FROM adstraffic.daily_ad_serving_stats WHERE full_date='2013-07-01';

# import aggregate data from file to database postgres
* * * * * sleep 20;psql -d warehouse -c "SELECT * FROM control.fn_refresh_import_aggregate_data()" >/dev/null 2>&1
* * * * * sleep 40;psql -d warehouse -c "SELECT * FROM control.fn_refresh_import_aggregate_data()" >/dev/null 2>&1

mkdir warehouse_aggregate_files
mkdir warehouse_aggregate_files/done
mkdir warehouse_aggregate_files/error
chmod -R 775 warehouse_aggregate_files

------------- Insert a new table to subscript tion
-- Insert new table to data source tables
INSERT INTO control.spctl_data_source_tables
(
  table_type,
  table_name
)
VALUES
(
  'DLA',
  'adstraffic.daily_event_stats_by_flight'
);

-- Run this query to generate insert query to data file config
SELECT 'INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,data_subject_id,export_module_id,data_source_table_id) VALUES ('''||table_name||''',''csv'','''','''',6,1,'||data_source_table_id||');' 
FROM control.spctl_data_source_tables 
WHERE table_name='adstraffic.daily_event_stats_by_flight'
ORDER BY table_name

-- create a data file config
INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,data_subject_id,export_module_id,data_source_table_id) VALUES ('adstraffic.daily_event_stats_by_flight','csv','','',1,1,568);
-- connect data file config into a subscription
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (19,116);
