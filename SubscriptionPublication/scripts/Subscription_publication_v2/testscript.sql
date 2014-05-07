INSERT INTO control.spctl_data_source_tables
(
  table_type,
  table_name
)
VALUES
(
  'DLA',
  'adsops.daily_agg_verve_ads_by_remnant'
);

-- Run this query to generate insert query to data file config
SELECT 'INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,data_subject_id,export_module_id,data_source_table_id) VALUES ('''||table_name||''',''csv'','''','''',6,1,'||data_source_table_id||');' 
FROM control.spctl_data_source_tables 
WHERE table_name='adsops.daily_agg_verve_ads_by_remnant'
ORDER BY table_name

-- create a data file config use export module to csv
INSERT INTO control.spctl_data_file_config(df_config_name,df_config_format,dt_desc,df_source_file,data_subject_id,export_module_id,data_source_table_id) VALUES ('adsops.daily_agg_verve_ads_by_remnant','csv','','',5,1,580);

-- create a data file config use export module to jaspert
INSERT INTO control.spctl_data_file_config(
					df_config_name,
					df_config_format,
					dt_desc,
					df_source_file,
					data_subject_id,
					export_module_id,
					df_attribute,
					data_source_table_id) 
					VALUES (
					'Daily Verve Ads V1 - Date Range csv',
					'csv',
					'',
					'/home/postgres/bin/subscription_publication/jasperReportTemplates/verve_ads_ops/jaspers/daily_verve_ads_v1/date_range/daily_verve_ads.jrxml',
					7,
					2,
					'roll_back_date=30 p_start_date={v_start_full_date} p_end_date={full_date}',
					580);
-- connect data file config into a subscription
INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) 
VALUES (27,182);

--Insert 1 customer get report via email
INSERT INTO control.spctl_pub_customer(customer_name,customer_desc,folder_content_transfer_script,transfer_script_name) 
VALUES ('Daily Verve Ads Group','Group users will receive Daily Verve Ads report by email','/home/postgres/bin/subscription_publication','publicToEmail.pl');
INSERT INTO control.spctl_customer_contact(customer_email,customer_phone_number,customer_key)
VALUES ('chinh.nguyen@ecepvn.org','+84982777098',3);
INSERT INTO control.spctl_customer_contact(customer_email,customer_phone_number,customer_key)
VALUES ('tho.hoang@vervemobile.com','',3);
INSERT INTO control.spctl_customer_contact(customer_email,customer_phone_number,customer_key)
VALUES ('nhut@ecepvn.org','',3);
--insert subscription for this customer. Every customer can be have many subscription
INSERT INTO control.spctl_pub_customer_subscription(subscription_name,subscription_desc,frequence,zip_before_transfer,customer_key)
VALUES ('Daily Verve Ads Report','','DAILY',TRUE,3);
INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (27);

UPDATE control.spctl_subscription_publication_process SET process_status ='WT' WHERE publication_process_id=3316;
 UPDATE control.spctl_subscription_publication_process_concurrent_trans SET status ='ER' WHERE publication_process_id=3316;


UPDATE control.spctl_data_file_config SET df_attribute='' WHERE df_config_id IN (176,177,178);
UPDATE control.spctl_pub_customer_article SET articel_status='INACTIVE' WHERE customer_article_key IN (121,122,119,117);

SELECT * FROM control.fn_spctl_insert_subscription_to_process(26,'date=2014-03-01',true);
SELECT * FROM control.fn_spctl_insert_subscription_to_process(27,'date=2014-03-02',true);
SELECT * FROM control.fn_spctl_insert_subscription_to_process(27,'start_date=2014-03-01&end_date=2014-03-04',true);

UPDATE control.spctl_data_file_config SET df_config_name='Daily Verve Ads V1 - Date Range XLS' WHERE df_config_id =179;
UPDATE control.spctl_pub_customer_subscription SET subscription_desc='Attached is Daily Verve Ads Report V1' WHERE subscription_key=27;

UPDATE control.spctl_customer_contact SET customer_contact_status='ACTIVE' WHERE customer_contact_id IN (3,4);