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
VALUES ('Daily Ad Serving Stats Aggregate','','DAILY',FALSE,1);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (1);

INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Monthly Aggregate','','MONTHLY',FALSE,1);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (2);



INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Daily Adcel reports','','DAILY',FALSE,2);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (2);

--- create export module
INSERT INTO control.spctl_export_module(module_desc,bin_dir,script_name,export_dir,export_file_name_format) VALUES ('This script export daily data from table','/home/postgres/bin/subscription_publication','export_daily_agg_data.pl','/data2/outgoing/daily_aggregate','export_mode.table_name.export_date.md5.timecode.csv');

INSERT INTO control.spctl_export_module(module_desc,bin_dir,script_name,export_dir,export_file_name_format) VALUES ('This script used to generate jasper report','/home/postgres/bin/subscription_publication','export_jasper_report.pl','/data2/outgoing/jasper_reports','reportName.date.md5.format');

--- create subject
INSERT INTO control.spctl_subject(subject_name,subject_desc) VALUES ('Testing Enviroment','This subject content all testing job');
--- create job
INSERT INTO control.spctl_job(job_name,job_desc,job_category,email_id,subject_id) VALUES ('Daily Adcel aggregate - send to S3','Export daily agg tables used for Daily Adcel reports','Adcel reports','chinh.nguyen@ecepvn.org',1);

INSERT INTO control.spctl_job(job_name,job_desc,job_category,email_id,subject_id) VALUES ('Daily Adcel reports - send to email','Send Adcel reports daily to customer','Adcel reports','chinh.nguyen@ecepvn.org',1);



--insert data file config

SELECT 'INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,job_id,export_module_id,data_source_table_id) VALUES ('''||table_name||''',''csv'','''','''',6,1,'||data_source_table_id||');' 
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
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1,'mode=daily date=2013-06-14',true);
SELECT * FROM control.spctl_subscription_publication_process_concurrent_trans WHERE publication_process_id=1;
SELECT * FROM control.spctl_subscription_publication_process;

UPDATE control.spctl_subscription_publication_process SET process_status='WT';
UPDATE control.spctl_subscription_publication_process_concurrent_trans SET status='ER' WHERE publication_process_id=1;
UPDATE control.spctl_subscription_publication_process SET process_status='TR' WHERE publication_process_id=4;
SELECT * FROM control.spctl_subscription_publication_process WHERE process_status<>'SU' AND process_status<>'WT';
SELECT * FROM control.spctl_data_source_tables
----View job and data_file_config
SELECT * FROM control.spctl_job;
SELECT b.job_name,a.df_config_name,a.df_config_id FROM control.spctl_data_file_config a,control.spctl_job b WHERE a.job_id=b.job_id AND b.job_id=6;
------------------
-- View subacription and data file
SELECT * FROM control.spctl_pub_customer_subscription;
UPDATE control.spctl_pub_customer_subscription SET zip_before_transfer=true;
SELECT a.subscription_name,c.df_config_name FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer_article b,control.spctl_data_file_config c
WHERE a.subscription_key=b.subscription_key AND b.df_config_id=c.df_config_id AND a.subscription_key=2;

SELECT a.subscription_name,a.subscription_key,c.df_config_name,b.articel_status FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer_article b,control.spctl_data_file_config c
WHERE a.subscription_key=b.subscription_key AND b.df_config_id=c.df_config_id 
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
SELECT * FROM control.fn_spctl_insert_subscription_to_process(1,'mode=date_range start_date=2013-06-01 end_date=2013-06-16',true);

SELECT * FROM control.fn_spctl_insert_subscription_to_process(2,'mode=daily calendar_year_month=2013-Mar',true);

SELECT * FROM control.spctl_subscription_publication_process_concurrent_trans WHERE publication_process_id=9;

SELECT * FROM control.spctl_subscription_publication_process WHERE publication_process_id=9;

UPDATE control.spctl_subscription_publication_process_concurrent_trans SET status='ER' WHERE publication_process_id=9 AND customer_article_key=16;

UPDATE control.spctl_subscription_publication_process SET process_status='TR' WHERE publication_process_id=4;
UPDATE control.spctl_subscription_publication_process SET process_actribute='mode=monthly calendar_year_month=2013-Apr' WHERE publication_process_id=9;

UPDATE control.spctl_pub_customer_article SET articel_status='INACTIVE' WHERE customer_article_key NOT IN (16) AND subscription_key=2;