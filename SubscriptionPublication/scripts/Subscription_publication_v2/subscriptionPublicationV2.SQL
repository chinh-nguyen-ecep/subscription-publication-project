/*
Created: 5/27/2013
Modified: 8/9/2013
Project: Subscription Pulication
Model: PostgreSQL 9.0
Company: Ecepvn.org
Version: 1.0
Database: PostgreSQL 9.0
*/


-- Drop relationships section -------------------------------------------------

ALTER TABLE "control"."spctl_subscription_publication_checkpoint" DROP CONSTRAINT "fk_spctl_subscription_publication_checkpoint"
;
ALTER TABLE "control"."spctl_data_file_config" DROP CONSTRAINT "fk_data_sub_data_file_config"
;
ALTER TABLE "control"."spctl_customer_contact" DROP CONSTRAINT "fk_spctl_customer_contact"
;
ALTER TABLE "control"."spctl_data_file_config" DROP CONSTRAINT "fk_spctl_data_file_config_data_source_tables"
;
ALTER TABLE "control"."spctl_data_file_config" DROP CONSTRAINT "fk_data_file_config_export_module"
;
ALTER TABLE "control"."spctl_subscription_publication_process" DROP CONSTRAINT "fk_subscription_process_notice"
;
ALTER TABLE "control"."spctl_subscription_publication_process_concurrent_trans" DROP CONSTRAINT "pfk_spctl_process_concurrent_trans_acticle"
;
ALTER TABLE "control"."spctl_subscription_publication_process_concurrent_trans" DROP CONSTRAINT "pfk_spctl_subscription_process_concurrent_trans"
;
ALTER TABLE "control"."spctl_subscription_publication_process" DROP CONSTRAINT "fk_spctl_subscription_process"
;
ALTER TABLE "control"."spctl_pub_customer_subscription" DROP CONSTRAINT "fk_spctl_pub_customer_subscription"
;
ALTER TABLE "control"."spctl_pub_customer_article" DROP CONSTRAINT "fk_spctl_aticle_df_config"
;
ALTER TABLE "control"."spctl_pub_customer_article" DROP CONSTRAINT "fk_spctl_customer_sub_article"
;




-- Drop keys for tables section -------------------------------------------------

ALTER TABLE "control"."spctl_customer_contact" DROP CONSTRAINT "pk_spctl_customer_contact"
;
ALTER TABLE "control"."spctl_data_source_tables" DROP CONSTRAINT "pk_data_source_tables"
;
ALTER TABLE "control"."spctl_data_source_tables" DROP CONSTRAINT "table_name"
;
ALTER TABLE "control"."spctl_export_module" DROP CONSTRAINT "pk_spctl_export_module"
;
ALTER TABLE "control"."spctl_notice" DROP CONSTRAINT "pk_spctl_notice"
;
ALTER TABLE "control"."spctl_subscription_publication_process_concurrent_trans" DROP CONSTRAINT "pk_spctl_subscription_publication_process_concurrent_trans"
;
ALTER TABLE "control"."spctl_subscription_publication_process" DROP CONSTRAINT "pk_spctl_subscription_publication_process"
;
ALTER TABLE "control"."spctl_pub_customer_article" DROP CONSTRAINT "pk_spctl_pub_customer_articale"
;
ALTER TABLE "control"."spctl_pub_customer_article" DROP CONSTRAINT "pk_spctl_pub_customer_article_02"
;
ALTER TABLE "control"."spctl_pub_customer_subscription" DROP CONSTRAINT "pk_spclt_pub_customer_subscription"
;
ALTER TABLE "control"."spctl_pub_customer_subscription" DROP CONSTRAINT "pk_spctl_pub_customer_subscription_01"
;
ALTER TABLE "control"."spctl_data_file_config" DROP CONSTRAINT "pk_spctl_data_file_config"
;
ALTER TABLE "control"."spctl_data_file_config" DROP CONSTRAINT "pk_spctl_data_file_config_01"
;
ALTER TABLE "control"."spctl_data_subject" DROP CONSTRAINT "pk_spctl_subject"
;
ALTER TABLE "control"."spctl_data_subject" DROP CONSTRAINT "data_subject_name"
;
ALTER TABLE "control"."spctl_pub_customer" DROP CONSTRAINT "spctl_pub_customer_pk"
;
ALTER TABLE "control"."spctl_pub_customer" DROP CONSTRAINT "customer_name"
;


-- Drop indexes section -------------------------------------------------

DROP INDEX "control"."idx_spctl_customer_contact"
;
DROP INDEX "control"."idx_spctl_customer_contact_02"
;
DROP INDEX "control"."idx_spctl_data_source_tables01"
;
DROP INDEX "control"."idx_spctl_data_source_tables02"
;
DROP INDEX "control"."idx_spctl_subscription_publication_checkpoint01"
;
DROP INDEX "control"."idx_spctl_export_module_01"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_concurrent_trans_01"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_concurrent_trans_02"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_concurrent_trans_03"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_concurrent_trans_04"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_01"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_02"
;
DROP INDEX "control"."idx_spctl_subscription_publication_process_03"
;
DROP INDEX "control"."idx_spctl_pub_customer_articale01"
;
DROP INDEX "control"."idx_spctl_pub_customer_article_02"
;
DROP INDEX "control"."idx_spctl_pub_customer_article_03"
;
DROP INDEX "control"."idx_spctl_pub_customer_article_04"
;
DROP INDEX "control"."idx_spclt_pub_customer_subscription01"
;
DROP INDEX "control"."idx_spctl_pub_customer_subscription_02"
;
DROP INDEX "control"."idx_spclt_data_file_config01"
;
DROP INDEX "control"."idx_spctl_data_file_config_02"
;
DROP INDEX "control"."idx_spctl_data_file_config_03"
;
DROP INDEX "control"."idx_spctl_data_file_config_04"
;
DROP INDEX "control"."idx_spclt_data_subject01"
;

-- Drop tables section ---------------------------------------------------

DROP TABLE "control"."spctl_customer_contact"
;
DROP TABLE "control"."spctl_data_source_tables"
;
DROP TABLE "control"."spctl_subscription_publication_checkpoint"
;
DROP TABLE "control"."spctl_export_module"
;
DROP TABLE "control"."spctl_notice"
;
DROP TABLE "control"."spctl_subscription_publication_process_concurrent_trans"
;
DROP TABLE "control"."spctl_subscription_publication_process"
;
DROP TABLE "control"."spctl_pub_customer_article"
;
DROP TABLE "control"."spctl_pub_customer_subscription"
;
DROP TABLE "control"."spctl_data_file_config"
;
DROP TABLE "control"."spctl_data_subject"
;
DROP TABLE "control"."spctl_pub_customer"
;

-- Drop functions section --------------------------------------------------- 

DROP FUNCTION "control"."fn_spctl_connect_subscription_to_data_file"( character varying,  character varying,  text)
;
DROP FUNCTION "control"."fn_spctl_refresh_update_subscription_process_status"()
;
DROP FUNCTION "control"."fn_spctl_refresh_update_data_up_to_date"()
;
DROP FUNCTION "control"."fn_spctl_refresh_public_export_file"()
;
DROP FUNCTION "control"."fn_spctl_refresh_export_data_file"()
;
DROP FUNCTION "control"."fn_spctl_insert_subscription_to_process"( bigint,  character varying,  boolean)
;
DROP FUNCTION "control"."fn_spctl_insert_subscription_to_process"( bigint)
;

-- Create functions section -------------------------------------------------

-- Function: control.fn_spctl_insert_subscription_to_process(bigint)

-- DROP FUNCTION control.fn_spctl_insert_subscription_to_process(bigint);
           
CREATE OR REPLACE FUNCTION control.fn_spctl_insert_subscription_to_process(p_subscription_key bigint)
  RETURNS character varying AS
$BODY$

/*
||
|| Function Name: fn_spctl_insert_subscription_to_process(bigint)
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-05
||
||
|| Description: 
||
||
|| Parameters: 
||    IN p_subscription_key    integer
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||


SELECT * FROM control.fn_spctl_insert_subscription_to_process(1);
*/

DECLARE
 rec            RECORD;
 v_result    character varying;
 v_subscription_process_id    bigint;
 v_process_count integer;
 v_is_active character varying;

BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_insert_subscription_to_process(%)',timeofday(),p_subscription_key;
v_result:='ER';
v_process_count:=0;
v_subscription_process_id:=-100;
v_is_active:='';

SELECT subscription_status INTO v_is_active FROM control.spctl_pub_customer_subscription WHERE subscription_key=p_subscription_key;
IF v_is_active<>'ACTIVE' THEN
    RAISE NOTICE 'This Subscription % is InActive',p_subscription_key;
    RETURN v_result;
END IF;

SELECT COUNT(1) INTO v_process_count FROM control.spctl_subscription_publication_process WHERE subscription_key=p_subscription_key AND process_status<>'SU';
IF v_process_count>0 THEN
    RAISE NOTICE 'Has a process on Subscription % is runing',p_subscription_key;
    RETURN v_result;
END IF;

IF v_process_count=0 AND v_is_active='ACTIVE' THEN
    INSERT INTO control.spctl_subscription_publication_process(subscription_key) VALUES (p_subscription_key) RETURNING publication_process_id INTO v_subscription_process_id;
    --Insert article
    INSERT INTO control.spctl_subscription_publication_process_concurrent_trans(publication_process_id,customer_article_key) 
    SELECT v_subscription_process_id,a.customer_article_key 
    FROM control.spctl_pub_customer_article a,control.spctl_data_file_config b,control.spctl_data_subject c
    WHERE a.subscription_key=p_subscription_key 
        AND a.articel_status='ACTIVE'
        AND b.df_status='ACTIVE'
        AND c.data_subject_status='ACTIVE'
        AND a.df_config_id=b.df_config_id
        AND b.data_subject_id=c.data_subject_id;
    --Update to checkpoint
    SELECT COUNT(1) INTO v_process_count FROM control.spctl_subscription_publication_checkpoint WHERE subscription_key=p_subscription_key;
    IF v_process_count=0 THEN
        INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (p_subscription_key);
    END IF;
    UPDATE control.spctl_subscription_publication_checkpoint a
    SET last_fire_time=now()
    WHERE a.subscription_key=p_subscription_key;
    -- end update checkpoint
    v_result:=v_subscription_process_id;
END IF;

RETURN v_result;

RAISE DEBUG 'ENDING: % fn_spctl_insert_subscription_to_process(%)',timeofday(),p_subscription_key;

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_insert_subscription_to_process(%); SQLSTATE=% SQLERRM=%',timeofday(),p_subscription_key,SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_refresh_load_adcel_data',SQLSTATE,SQLERRM);
RETURN v_result;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


;

ALTER FUNCTION control.fn_spctl_insert_subscription_to_process(bigint)
  OWNER TO warehouse_dbo;

-- Function: control.fn_spctl_insert_subscription_to_process(bigint, character varying, boolean)

-- DROP FUNCTION control.fn_spctl_insert_subscription_to_process(bigint, character varying, boolean);

CREATE OR REPLACE FUNCTION control.fn_spctl_insert_subscription_to_process(p_subscription_key bigint, p_process_actribute character varying, p_export_now boolean)
  RETURNS character varying AS
$BODY$

/*
||
|| Function Name: fn_spctl_insert_subscription_to_process(bigint)
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-25
||
||
|| Description: 
||
||
|| Parameters: 
||    IN p_subscription_key    integer
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||


SELECT * FROM control.fn_spctl_insert_subscription_to_process(1,'mode=date_range start_date=2013-06-01 end_date=2013-06-16',true);

SELECT * FROM control.fn_spctl_insert_subscription_to_process(2,'mode=daily calendar_year_month=2013-Mar',true);

SELECT * FROM control.spctl_subscription_publication_process_concurrent_trans WHERE publication_process_id=9;

SELECT * FROM control.spctl_subscription_publication_process WHERE publication_process_id=9;

UPDATE control.spctl_subscription_publication_process_concurrent_trans SET status='ER' WHERE publication_process_id=9 AND customer_article_key=16;

UPDATE control.spctl_subscription_publication_process SET process_status='TR' WHERE publication_process_id=4;
UPDATE control.spctl_subscription_publication_process SET process_actribute='mode=monthly calendar_year_month=2013-Apr' WHERE publication_process_id=9;

UPDATE control.spctl_pub_customer_article SET articel_status='INACTIVE' WHERE customer_article_key NOT IN (16) AND subscription_key=2;
*/

DECLARE
 rec            RECORD;
 v_result    character varying;
 v_subscription_process_id    bigint;
 v_process_count integer;
 v_is_active character varying;
 v_export_status character varying;
BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_insert_subscription_to_process(%)',timeofday(),p_subscription_key;
v_result:='ER';
v_process_count:=0;
v_subscription_process_id:=-100;
v_is_active:='';

SELECT subscription_status INTO v_is_active FROM control.spctl_pub_customer_subscription WHERE subscription_key=p_subscription_key;
IF v_is_active<>'ACTIVE' THEN
    RAISE NOTICE 'This Subscription % is InActive',p_subscription_key;
    RETURN v_result;
END IF;

SELECT COUNT(1) INTO v_process_count FROM control.spctl_subscription_publication_process WHERE subscription_key=p_subscription_key AND process_status<>'SU';
IF v_process_count>0 THEN
    RAISE NOTICE 'Has a process on Subscription % is runing',p_subscription_key;
    RETURN v_result;
END IF;

IF v_process_count=0 AND v_is_active='ACTIVE' THEN
    INSERT INTO control.spctl_subscription_publication_process(subscription_key,process_actribute) VALUES (p_subscription_key,p_process_actribute) RETURNING publication_process_id INTO v_subscription_process_id;
    --Insert article
    v_export_status:='WT';
    IF p_export_now=TRUE THEN v_export_status:='ER';
    END IF;
    
    INSERT INTO control.spctl_subscription_publication_process_concurrent_trans(publication_process_id,customer_article_key,status) 
    SELECT v_subscription_process_id
        ,a.customer_article_key 
        ,v_export_status::bpchar
    FROM control.spctl_pub_customer_article a,control.spctl_data_file_config b,control.spctl_data_subject c
    WHERE a.subscription_key=p_subscription_key 
        AND a.articel_status='ACTIVE'
        AND b.df_status='ACTIVE'
        AND c.data_subject_status='ACTIVE'
        AND a.df_config_id=b.df_config_id
        AND b.data_subject_id=c.data_subject_id;
    --Update to checkpoint
    SELECT COUNT(1) INTO v_process_count FROM control.spctl_subscription_publication_checkpoint WHERE subscription_key=p_subscription_key;
    IF v_process_count=0 THEN
        INSERT INTO control.spctl_subscription_publication_checkpoint(subscription_key) VALUES (p_subscription_key);
    END IF;
    UPDATE control.spctl_subscription_publication_checkpoint a
    SET last_fire_time=now()
    WHERE a.subscription_key=p_subscription_key;
    -- end update checkpoint
    v_result:=v_subscription_process_id;
END IF;

RETURN v_result;

RAISE DEBUG 'ENDING: % fn_spctl_insert_subscription_to_process(%)',timeofday(),p_subscription_key;

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_insert_subscription_to_process(%,%,%); SQLSTATE=% SQLERRM=%',timeofday(),p_subscription_key,p_process_actribute ,p_export_now,SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_refresh_load_adcel_data',SQLSTATE,SQLERRM);
RETURN v_result;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
;

ALTER FUNCTION control.fn_spctl_insert_subscription_to_process(bigint, character varying, boolean)
  OWNER TO warehouse_dbo;

-- Function: control.fn_spctl_refresh_export_data_file()

-- DROP FUNCTION control.fn_spctl_refresh_export_data_file();

CREATE OR REPLACE FUNCTION control.fn_spctl_refresh_export_data_file()
  RETURNS void AS
$BODY$

/*
||
|| Function Name: fn_spctl_refresh_export_data_file()
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-07
||
||
|| Description: 
||
||
|| Parameters: 
||    
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||


SELECT * FROM control.fn_spctl_refresh_export_data_file();
*/

DECLARE
 rec            RECORD;
 v_count_export_process_running integer;
 v_cmd character varying;
 v_cmd_result character varying;
 v_result character varying;
 v_max_process_running_at_a_time integer;
 v_publication_process_status character varying;
 v_publication_process_id bigint;
 v_customer_article_key bigint;
BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_refresh_export_data_file()',timeofday();
v_result:='ER';
v_count_export_process_running:=0;
v_cmd:='';
v_max_process_running_at_a_time:=3;

-- Count number of export process are runing
SELECT COUNT(1) INTO v_count_export_process_running FROM control.spctl_subscription_publication_process_concurrent_trans WHERE status='PSE';
IF(v_count_export_process_running=v_max_process_running_at_a_time) THEN
    RAISE NOTICE 'Maximun process: %',v_max_process_running_at_a_time;
    RETURN ;
END IF;

-- Get data input to process
v_publication_process_id:=0;
v_customer_article_key:=0;

SELECT 
publication_process_id,customer_article_key INTO v_publication_process_id,v_customer_article_key
FROM control.spctl_subscription_publication_process_concurrent_trans 
WHERE status='ER'
ORDER BY publication_process_id,customer_article_key LIMIT 1;
IF (v_publication_process_id>0 AND v_customer_article_key>0) THEN
    SELECT 
    e.bin_dir
    ,e.script_name
    ,g.create_date::date-1 as report_date
    INTO rec
    FROM control.spctl_subscription_publication_process_concurrent_trans a
        ,control.spctl_pub_customer_article b
        ,control.spctl_data_file_config c
        ,control.spctl_export_module e
        ,control.spctl_pub_customer_subscription f
        ,control.spctl_subscription_publication_process g
    WHERE a.status='ER'
    AND b.articel_status='ACTIVE'
    AND c.df_status='ACTIVE'
    AND a.customer_article_key=b.customer_article_key
    AND b.df_config_id=c.df_config_id
    AND c.export_module_id=e.export_module_id
    AND g.publication_process_id=a.publication_process_id
    AND g.subscription_key=f.subscription_key
    AND a.publication_process_id=v_publication_process_id
    AND a.customer_article_key=v_customer_article_key
    ;
    RAISE NOTICE '%,%',v_publication_process_id,v_customer_article_key;
    IF rec IS NOT NULL THEN
        v_cmd:='cd '||rec.bin_dir||' && perl '||rec.script_name||' '||v_publication_process_id||' '||v_customer_article_key;
        
        RAISE NOTICE '%',v_cmd;
        
        v_cmd:=' SELECT * FROM staging.fn_cmd_shell('''||v_cmd||''')';
        
        EXECUTE v_cmd;
        RAISE NOTICE '%',v_cmd;
        SELECT process_status INTO v_publication_process_status FROM control.spctl_subscription_publication_process WHERE publication_process_id=v_publication_process_id;
        IF v_publication_process_status='WT' THEN
            UPDATE control.spctl_subscription_publication_process
            SET process_status='PS'
            WHERE publication_process_id=v_publication_process_id;
        END IF;        
    END IF;

END IF;

RETURN ;

RAISE DEBUG 'ENDING: % fn_spctl_refresh_export_data_file()',timeofday();

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_refresh_export_data_file(); SQLSTATE=% SQLERRM=%',timeofday(),SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_refresh_load_adcel_data',SQLSTATE,SQLERRM);
        UPDATE control.spctl_subscription_publication_process_concurrent_trans 
        SET status='EF',error_message=SQLSTATE||' '||SQLERRM,dt_lastchange=now()
        WHERE publication_process_id=v_publication_process_id AND customer_article_key=v_customer_article_key;
RETURN ;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION control.fn_spctl_refresh_export_data_file()
  OWNER TO warehouse_dbo;

;

ALTER FUNCTION control.fn_spctl_refresh_export_data_file()
  OWNER TO warehouse_dbo;

-- Function: control.fn_spctl_refresh_public_export_file()

-- DROP FUNCTION control.fn_spctl_refresh_public_export_file();

CREATE OR REPLACE FUNCTION control.fn_spctl_refresh_public_export_file()
  RETURNS void AS
$BODY$

/*
||
|| Function Name: fn_spctl_refresh_public_export_file()
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-07
||
||
|| Description: 
||
||
|| Parameters: 
||    
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||


SELECT * FROM control.fn_spctl_refresh_public_export_file();
*/

DECLARE
 rec            RECORD;
 v_count_public_process_running integer;
 v_cmd character varying;
 v_cmd_result character varying;
 v_result character varying;
 v_max_process_running_at_a_time integer;
BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_refresh_public_export_file()',timeofday();
v_result:='ER';
v_count_public_process_running:=0;
v_cmd:='';
v_max_process_running_at_a_time:=2;

-- Count number of export process are runing
SELECT COUNT(1) INTO v_count_public_process_running FROM control.spctl_subscription_publication_process WHERE process_status='PST';
IF(v_count_public_process_running=v_max_process_running_at_a_time) THEN
    RETURN ;
ELSE
    SELECT b.publication_process_id
    ,c.folder_content_transfer_script
    ,c.transfer_script_name
    INTO rec
    FROM     control.spctl_pub_customer_subscription a
        ,control.spctl_subscription_publication_process b
        ,control.spctl_pub_customer c
    WHERE b.process_status='TR'
    AND b.subscription_key=a.subscription_key
    AND a.customer_key=c.customer_key    
    ORDER BY b.publication_process_id LIMIT 1;

    IF rec IS NOT NULL THEN
        v_cmd:='cd '||rec.folder_content_transfer_script||' && perl '||rec.transfer_script_name||' '||rec.publication_process_id;
        
        RAISE NOTICE '%',v_cmd;
        
        v_cmd:=' SELECT * FROM staging.fn_cmd_shell('''||v_cmd||''')';
        
        EXECUTE v_cmd;
    END IF;

    
END IF;

RETURN ;

RAISE DEBUG 'ENDING: % fn_spctl_refresh_public_export_file()',timeofday();

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_refresh_public_export_file(); SQLSTATE=% SQLERRM=%',timeofday(),SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_refresh_load_adcel_data',SQLSTATE,SQLERRM);
        UPDATE control.spctl_subscription_publication_process
        SET process_status='TF',dt_lastchange=now()
        WHERE publication_process_id=rec.publication_process_id;
RETURN ;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


;

ALTER FUNCTION control.fn_spctl_refresh_public_export_file()
  OWNER TO warehouse_dbo;

-- Function: control.fn_spctl_refresh_update_data_up_to_date()

-- DROP FUNCTION control.fn_spctl_refresh_update_data_up_to_date();

CREATE OR REPLACE FUNCTION control.fn_spctl_refresh_update_data_up_to_date()
  RETURNS void AS
$BODY$

/*
||
|| Function Name: fn_spctl_refresh_update_data_up_to_date()
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-07
||
||
|| Description: 
||
||
|| Parameters: 
||    
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||
UPDATE control.spctl_data_source_tables SET date_up_to_date=null;

SELECT * FROM refer.date_dim WHERE full_date <= now()::date ORDER BY date_sk desc LIMIT 10;
SELECT * FROM control.fn_spctl_refresh_update_data_up_to_date();
*/

DECLARE
 rec            RECORD;
 v_count integer;
 v_checkCode character varying;
 v_cmd character varying;
 v_date_up_to_date_DIM_vailable date;
 v_date_up_to_date_DLA_vailable date;
 v_date_up_to_date_WLA_vailable date;
 v_date_up_to_date_MLA_vailable date;

 v_week_of_year_monday_up_to_date integer;
 v_month_since_2005_up_to_date integer;
 v_is_locked boolean;
BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_refresh_update_data_up_to_date()',timeofday();
v_count:=0;
v_cmd:='';
--Check if table is updating
SELECT COUNT(1)  INTO v_count FROM pg_locks l, pg_stat_all_tables t where l.relation=t.relid AND t.relname = 'spctl_data_source_tables';
IF v_count>0 THEN 
    v_is_locked:=TRUE;
    RAISE NOTICE 'Table control.spctl_data_source_tables is locked.';
    RETURN ;
END IF;
-- Get current up to date
v_date_up_to_date_DIM_vailable:=now()::date;
v_date_up_to_date_DLA_vailable:=now()::date-1;

SELECT week_of_year_monday INTO v_week_of_year_monday_up_to_date FROM refer.date_dim WHERE full_date=now()::date;
SELECT month_since_2005 INTO v_month_since_2005_up_to_date FROM refer.date_dim WHERE full_date=now()::date;    
SELECT max(full_date) INTO v_date_up_to_date_WLA_vailable FROM refer.date_dim WHERE week_of_year_monday=v_week_of_year_monday_up_to_date-1 AND calendar_year=EXTRACT(YEAR FROM now()::date) GROUP BY week_of_year_monday;
SELECT max(full_date) INTO v_date_up_to_date_MLA_vailable FROM refer.date_dim WHERE month_since_2005=v_month_since_2005_up_to_date-1 GROUP BY month_since_2005;    

RAISE NOTICE 'v_date_up_to_date_DIM_vailable: %',v_date_up_to_date_DIM_vailable;
RAISE NOTICE 'v_date_up_to_date_DLA_vailable: %',v_date_up_to_date_DLA_vailable;
RAISE NOTICE 'v_date_up_to_date_WLA_vailable: %',v_date_up_to_date_WLA_vailable;
RAISE NOTICE 'v_date_up_to_date_MLA_vailable: %',v_date_up_to_date_MLA_vailable;    

-- Update for daily agg DLA
FOR rec IN SELECT * FROM control.spctl_data_source_tables WHERE table_type='DLA' ORDER BY table_name LOOP
    --Check table in to information_schema.tables
    v_count:=0;
    v_checkCode:='';
    SELECT COUNT(1) INTO v_count FROM information_schema.tables GROUP BY table_schema,table_name HAVING table_schema||'.'||table_name = trim(both ' ' FROM rec.table_name) ;
    --Check table update
    SELECT md5(trim(both ' ' FROM n_tup_ins::text||n_tup_upd::text||n_tup_del::text)) INTO v_checkCode FROM pg_stat_all_tables WHERE schemaname||'.'||relname=trim(both ' ' FROM rec.table_name) ;
    --RAISE NOTICE 'Table: % newCheckCode: % oldCheckCode: %',rec.table_name,v_checkCode,rec."checkCode";
    IF v_count>0 AND (v_checkCode!=trim(both ' ' FROM rec."checkCode") OR rec."checkCode" IS NULL) THEN
        RAISE NOTICE 'Update table: %',rec.table_name;
        v_cmd:='
            UPDATE control.spctl_data_source_tables a
            SET date_up_to_date=b.date_up_to_date
            ,"checkCode"='''||v_checkCode||'''
            FROM
            (
                SELECT max(eastern_date_sk),max(a.full_date) as date_up_to_date 
                FROM '||rec.table_name||' a 
                WHERE a.is_active=true                  
            )b
            WHERE a.table_name='''||rec.table_name||''';                    
        ';
        --RAISE NOTICE 'v_cmd: %',v_cmd;
        
        BEGIN
            EXECUTE v_cmd ;                
        EXCEPTION WHEN raise_exception THEN
            RAISE NOTICE 'The query has procuded 0 rows: %',v_cmd; 
        END;
        --COMMIT;
        
    ELSE
        --PERFORM fn_email_failure_notification('fn_spctl_refresh_update_data_up_to_date - Table % do not exist in data base system',rec.table_name);
        IF v_count=0 THEN 
            RAISE NOTICE '% Do not exist',rec.table_name;
        ELSE 
            --RAISE NOTICE '% Not Update!',rec.table_name ;
        END IF;
        
    END IF;
    
    
END LOOP; 

-- Update for Week agg WLA
FOR rec IN SELECT * FROM control.spctl_data_source_tables WHERE table_type='WLA' ORDER BY table_name LOOP
    --Check table in to information_schema.tables
    v_count:=0;
    v_checkCode:='';
    SELECT COUNT(1) INTO v_count FROM information_schema.tables GROUP BY table_schema,table_name HAVING table_schema||'.'||table_name = trim(both ' ' FROM rec.table_name) ;
    --Check table update
    SELECT md5(trim(both ' ' FROM n_tup_ins::text||n_tup_upd::text||n_tup_del::text)) INTO v_checkCode FROM pg_stat_all_tables WHERE schemaname||'.'||relname=trim(both ' ' FROM rec.table_name) ;
    IF v_count>0 AND (v_checkCode!=trim(both ' ' FROM rec."checkCode") OR rec."checkCode" IS NULL) THEN
        RAISE NOTICE 'Update table: %',rec.table_name;
        v_cmd:='UPDATE control.spctl_data_source_tables a
            SET date_up_to_date=b.end_week
            ,week_up_to_date=b.year_week
            ,"checkCode"='''||v_checkCode||'''
            FROM
            (
                SELECT max(year_week) as year_week,max(end_week) as end_week
                FROM '||rec.table_name||' a
                WHERE is_active=true
            )b
            WHERE a.table_name='''||rec.table_name||''';

        ';
        --RAISE NOTICE 'v_cmd: %',v_cmd;
        EXECUTE v_cmd;
    ELSE
        --PERFORM fn_email_failure_notification('fn_spctl_refresh_update_data_up_to_date - Table % do not exist in data base system',rec.table_name);    
        IF v_count=0 THEN 
            RAISE NOTICE '% Do not exist',rec.table_name;
        ELSE 
            --RAISE NOTICE '% Not Update!',rec.table_name ;
        END IF;
    END IF;
    
END LOOP; 

-- Update for Monthly agg MLA
FOR rec IN SELECT * FROM control.spctl_data_source_tables WHERE table_type='MLA' ORDER BY table_name LOOP
    --Check table in to information_schema.tables
    v_count:=0;
    v_checkCode:='';
    SELECT COUNT(1) INTO v_count FROM information_schema.tables GROUP BY table_schema,table_name HAVING table_schema||'.'||table_name = trim(both ' ' FROM rec.table_name) ;
    --Check table update
    SELECT md5(trim(both ' ' FROM n_tup_ins::text||n_tup_upd::text||n_tup_del::text)) INTO v_checkCode FROM pg_stat_all_tables WHERE schemaname||'.'||relname=trim(both ' ' FROM rec.table_name) ;
    IF v_count>0 AND (v_checkCode!=trim(both ' ' FROM rec."checkCode") OR rec."checkCode" IS NULL) THEN
        RAISE NOTICE 'Update table: %',rec.table_name;
        v_cmd:='UPDATE control.spctl_data_source_tables a
            SET month_sine_2005_up_to_date=b.month_sine_2005_up_to_date
            ,"checkCode"='''||v_checkCode||'''
            FROM
            (
                SELECT max(a.month_since_2005) as month_sine_2005_up_to_date
                FROM '||rec.table_name||' a
                WHERE is_active=true AND month_since_2005!=32767
            )b
            WHERE a.table_name='''||rec.table_name||''';

            UPDATE control.spctl_data_source_tables a
            SET date_up_to_date=b.date_up_to_date            
            FROM
            (
                SELECT max(full_date) as date_up_to_date,month_since_2005   
                FROM refer.date_dim
                WHERE full_date<now()::date
                GROUP BY month_since_2005
            )b
            WHERE a.table_name='''||rec.table_name||'''
            AND a.month_sine_2005_up_to_date=b.month_since_2005;
            UPDATE control.spctl_data_source_tables a
            SET month_up_to_date=b.month_up_to_date            
            FROM
            (
                SELECT calendar_year_month as month_up_to_date,month_since_2005   
                FROM refer.date_dim
                WHERE full_date<now()::date
                GROUP BY month_since_2005,calendar_year_month
            )b
            WHERE a.table_name='''||rec.table_name||'''
            AND a.month_sine_2005_up_to_date=b.month_since_2005;
        ';
        --RAISE NOTICE 'v_cmd: %',v_cmd;
        EXECUTE v_cmd;
    ELSE
        --PERFORM fn_email_failure_notification('fn_spctl_refresh_update_data_up_to_date - Table % do not exist in data base system',rec.table_name);    
        IF v_count=0 THEN 
            RAISE NOTICE '% Do not exist',rec.table_name;
        ELSE 
            --RAISE NOTICE '% Not Update!',rec.table_name ;
        END IF;
    END IF;
    
END LOOP; 

-- Update for DIM 
FOR rec IN SELECT * FROM control.spctl_data_source_tables WHERE table_type='DIM' AND (date_up_to_date::date< v_date_up_to_date_DIM_vailable OR date_up_to_date IS NULL) ORDER BY table_name LOOP
    --Check table in to information_schema.tables
    v_count:=0;
    SELECT COUNT(1) INTO v_count FROM information_schema.tables GROUP BY table_schema,table_name HAVING table_schema||'.'||table_name = trim(both ' ' FROM rec.table_name) ;
    SELECT COUNT(1) INTO v_count FROM control.data_current_up_to_date GROUP BY table_name HAVING trim(both ' ' FROM 'refer.'||table_name) = trim(both ' ' FROM rec.table_name) ;
    
    IF v_count>0 THEN
        --RAISE NOTICE 'Update table: %',rec.table_name;
        v_cmd:='UPDATE control.spctl_data_source_tables a
            SET date_up_to_date=current_up_to
            FROM
            (
                SELECT max(current_up_to)::date as current_up_to
                FROM control.data_current_up_to_date a
                --WHERE table_type=''D'' 
                GROUP BY current_up_to,table_name
                HAVING ''refer.''||table_name='''||rec.table_name||'''
            )b
            WHERE a.table_name='''||rec.table_name||''';
        ';
        --RAISE NOTICE 'v_cmd: %',v_cmd;
        EXECUTE v_cmd;
    ELSE
        --PERFORM fn_email_failure_notification('fn_spctl_refresh_update_data_up_to_date - Table % do not exist in data base system',rec.table_name);    
        --RAISE NOTICE '% Do not exist',rec.table_name;
    END IF;
    
END LOOP; 


RETURN ;

RAISE DEBUG 'ENDING: % fn_spctl_refresh_update_data_up_to_date()',timeofday();

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_refresh_update_data_up_to_date(); SQLSTATE=% SQLERRM=%',timeofday(),SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_spctl_refresh_update_data_up_to_date',SQLSTATE,SQLERRM);
RETURN ;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

;

ALTER FUNCTION control.fn_spctl_refresh_update_data_up_to_date()
  OWNER TO warehouse_dbo;

-- Function: control.fn_spctl_refresh_update_subscription_process_status()

-- DROP FUNCTION control.fn_spctl_refresh_update_subscription_process_status();

CREATE OR REPLACE FUNCTION control.fn_spctl_refresh_update_subscription_process_status()
  RETURNS void AS
$BODY$

/*
||
|| Function Name: fn_spctl_refresh_update_subscription_process_status()
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-11
||
||
|| Description: 
||
||
|| Parameters: 
||    
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||


SELECT * FROM control.fn_spctl_refresh_update_subscription_process_status();
*/

DECLARE
 rec            RECORD;
 v_count_total integer;
 v_count_tr integer;
 v_publication_process_id bigint;
BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_refresh_update_subscription_process_status()',timeofday();

FOR rec IN SELECT * FROM control.spctl_subscription_publication_process WHERE process_status='PS' LOOP
    v_publication_process_id:=rec.publication_process_id;
        SELECT COUNT(1) INTO v_count_total FROM control.spctl_subscription_publication_process_concurrent_trans
        WHERE publication_process_id=v_publication_process_id;

    -- Don't have any trans Set process status to SU
    
    IF v_count_total=0 THEN
            UPDATE control.spctl_subscription_publication_process
            SET process_status='SU'
            WHERE publication_process_id=v_publication_process_id;
            RETURN;
    END IF;

        -- Get number of trans have status is 'TR'
        SELECT COUNT(1) INTO v_count_tr FROM control.spctl_subscription_publication_process_concurrent_trans
        WHERE publication_process_id=v_publication_process_id AND status='TR';    

        -- All trans have 'TR' status. We will star transfer process.    
        IF v_count_total=v_count_tr THEN
            UPDATE control.spctl_subscription_publication_process
            SET process_status='TR'
            WHERE publication_process_id=v_publication_process_id;
            RAISE NOTICE 'Set TR for Process: % ',v_publication_process_id;
        END IF;
END LOOP; 


RETURN ;

RAISE DEBUG 'ENDING: % fn_spctl_refresh_update_subscription_process_status()',timeofday();

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_refresh_update_subscription_process_status(); SQLSTATE=% SQLERRM=%',timeofday(),SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_refresh_load_adcel_data',SQLSTATE,SQLERRM);
RETURN ;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


;

ALTER FUNCTION control.fn_spctl_refresh_update_subscription_process_status()
  OWNER TO warehouse_dbo;

-- Function: control.fn_spctl_connect_subscription_to_data_file(character varying, character varying, text)

-- DROP FUNCTION control.fn_spctl_connect_subscription_to_data_file(character varying, character varying, text);

CREATE OR REPLACE FUNCTION control.fn_spctl_connect_subscription_to_data_file(p_customer_name character varying, p_sub_name character varying, p_data_file_config_name text)
  RETURNS character varying AS
$BODY$

/*
||
|| Function Name: fn_spctl_connect_subscription_to_data_file()
||
|| Author: Chinh Nguyen
||
|| Date: 2013-06-27
||
||
|| Description: This function create relationships for subscription to data file config. Insert one or many row in control.spctl_pub_customer_article
||
||
|| Parameters: 
||    
|| Return: void
||     - Failure case: 0
||         -- Error handling: send notification email
||


SELECT * FROM control.fn_spctl_connect_subscription_to_data_file('s3-amazon','City Grid','adnetwork.daily_cg_performance,adnetwork.daily_cg_mobile_performance');
*/

DECLARE
 v_count integer;
 v_cmd character varying;
 v_subscription_id bigint;
 v_data_file_config_id bigint;
 v_data_file_config_name_array character varying[];
 v_data_file_config_name character varying;
BEGIN

RAISE DEBUG 'STARTING: % fn_spctl_connect_subscription_to_data_file(character varying,character varying,text)',timeofday();

-- Load subscription id
v_subscription_id:=0;
SELECT subscription_key INTO v_subscription_id FROM control.spctl_pub_customer_subscription a,control.spctl_pub_customer b
WHERE a.customer_key=b.customer_key AND b.customer_name=p_customer_name AND a.subscription_name=p_sub_name;

IF v_subscription_id=0 OR v_subscription_id IS NULL THEN
    RAISE NOTICE 'Subscription % of customer % do not exist!',p_customer_name ,p_sub_name ;
    RETURN 'ER';
END IF;
RAISE NOTICE 'Subscription ID: %',v_subscription_id;

--
v_data_file_config_name_array:=string_to_array(p_data_file_config_name,',')::character varying;

FOR i IN array_lower(v_data_file_config_name_array, 1) .. array_upper(v_data_file_config_name_array, 1) LOOP
    v_data_file_config_name:=v_data_file_config_name_array[i];
    RAISE NOTICE 'data file config name: %',v_data_file_config_name;
    
    v_data_file_config_id:=0;
    SELECT df_config_id INTO v_data_file_config_id FROM control.spctl_data_file_config WHERE df_config_name=v_data_file_config_name;

    IF v_data_file_config_id=0 OR v_data_file_config_id IS NULL THEN    
        RAISE EXCEPTION 'Data file config % do not exist!', v_data_file_config_name
        USING HINT = 'Please check your data file config name';
    END IF;

    -- INSERT INTO control.spctl_pub_customer_article
    RAISE NOTICE 'INSERT subscription_key,df_config_id (%,%)',v_subscription_id,v_data_file_config_id;
    INSERT INTO control.spctl_pub_customer_article(subscription_key,df_config_id) VALUES (v_subscription_id,v_data_file_config_id);
END LOOP;

RETURN 'SU';

RAISE DEBUG 'ENDING: % fn_spctl_connect_subscription_to_data_file(character varying,character varying,text)',timeofday();

EXCEPTION
 WHEN others THEN
        RAISE NOTICE 'FAILED: % fn_spctl_connect_subscription_to_data_file(character varying,character varying,text); SQLSTATE=% SQLERRM=%',timeofday(),SQLSTATE,SQLERRM;
        --PERFORM fn_email_failure_notification('fn_refresh_load_adcel_data',SQLSTATE,SQLERRM);
RETURN 'ER';
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


;

ALTER FUNCTION control.fn_spctl_connect_subscription_to_data_file(character varying, character varying, text)
  OWNER TO warehouse_dbo;

-- Create tables section -------------------------------------------------

-- Table control.spctl_pub_customer

CREATE TABLE "control"."spctl_pub_customer"(
 "customer_key" BigSerial NOT NULL,
 "customer_name" Character varying(255) NOT NULL,
 "customer_type" Character varying(20) DEFAULT 'INTERNAL' NOT NULL,
 "customer_host_name" Character varying(50),
 "customer_destination_folder" Character varying(255),
 "customer_desc" Character varying(255),
 "customer_status" Character varying(255) DEFAULT 'ACTIVE' NOT NULL,
 "transfer_script_name" Character varying(255) NOT NULL,
 "folder_content_transfer_script" Character varying(255) NOT NULL
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_pub_customer" OWNER TO warehouse_dbo
;

-- Add keys for table control.spctl_pub_customer

ALTER TABLE "control"."spctl_pub_customer" ADD CONSTRAINT "spctl_pub_customer_pk" PRIMARY KEY ("customer_key")
 USING INDEX TABLESPACE "control_tablespace"
;

ALTER TABLE "control"."spctl_pub_customer" ADD CONSTRAINT "customer_name" UNIQUE ("customer_name")
;

-- Table control.spctl_data_subject

CREATE TABLE "control"."spctl_data_subject"(
 "data_subject_id" BigSerial NOT NULL,
 "data_subject_name" Character varying(255) NOT NULL,
 "data_subject_desc" Character varying(255),
 "data_subject_status" Character(100) DEFAULT 'ACTIVE'
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_data_subject" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_data_subject

CREATE INDEX "idx_spclt_data_subject01" ON "control"."spctl_data_subject" ("data_subject_id")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_data_subject

ALTER TABLE "control"."spctl_data_subject" ADD CONSTRAINT "pk_spctl_subject" PRIMARY KEY ("data_subject_id")
 USING INDEX TABLESPACE "control_tablespace"
;

ALTER TABLE "control"."spctl_data_subject" ADD CONSTRAINT "data_subject_name" UNIQUE ("data_subject_name")
;

-- Table control.spctl_data_file_config

CREATE TABLE "control"."spctl_data_file_config"(
 "df_config_id" BigSerial NOT NULL,
 "df_config_name" Character varying(255) NOT NULL,
 "df_config_format" Character varying(20) DEFAULT 'sql' NOT NULL,
 "dt_desc" Character varying(255),
 "df_status" Character varying(255) DEFAULT 'ACTIVE' NOT NULL,
 "df_source_file" Character varying(500) NOT NULL,
 "dt_column_list" Character varying(255),
 "df_attribute" Character varying(255),
 "export_module_id" Bigint NOT NULL,
 "data_source_table_id" Bigint NOT NULL,
 "data_subject_id" Bigint NOT NULL
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_data_file_config" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_data_file_config

CREATE INDEX "idx_spclt_data_file_config01" ON "control"."spctl_data_file_config" ("df_config_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_data_file_config_02" ON "control"."spctl_data_file_config" ("export_module_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_data_file_config_03" ON "control"."spctl_data_file_config" ("data_source_table_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_data_file_config_04" ON "control"."spctl_data_file_config" ("data_subject_id")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_data_file_config

ALTER TABLE "control"."spctl_data_file_config" ADD CONSTRAINT "pk_spctl_data_file_config" PRIMARY KEY ("df_config_id")
;

ALTER TABLE "control"."spctl_data_file_config" ADD CONSTRAINT "pk_spctl_data_file_config_01" UNIQUE ("df_config_name","data_subject_id","data_source_table_id","export_module_id")
 USING INDEX TABLESPACE "control_tablespace"
;

-- Table control.spctl_pub_customer_subscription

CREATE TABLE "control"."spctl_pub_customer_subscription"(
 "subscription_key" BigSerial NOT NULL,
 "subscription_name" Character varying(255) NOT NULL,
 "subscription_desc" Character varying(255),
 "subscription_status" Character varying(255) DEFAULT 'ACTIVE' NOT NULL,
 "subscription_attribute" Character varying(255),
 "frequence" Character varying(255) DEFAULT 'DAILY' NOT NULL,
 "zip_before_transfer" Boolean DEFAULT FALSE NOT NULL,
 "start_time" Time DEFAULT '09:00:00'::time NOT NULL,
 "delay_time" Time DEFAULT '10:00:00'::time NOT NULL,
 "day_of_week" Character varying(30),
 "date_of_month" Date,
 "auto_public" Boolean DEFAULT TRUE NOT NULL,
 "customer_key" Bigint NOT NULL
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;
COMMENT ON COLUMN "control"."spctl_pub_customer_subscription"."frequence" IS 'DAILY,WEEKLY,MONTHLY,OTHER'
;
COMMENT ON COLUMN "control"."spctl_pub_customer_subscription"."day_of_week" IS 'Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday'
;
COMMENT ON COLUMN "control"."spctl_pub_customer_subscription"."date_of_month" IS '1,2,3,4,5.....31'
;

alter table "control"."spctl_pub_customer_subscription" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_pub_customer_subscription

CREATE INDEX "idx_spclt_pub_customer_subscription01" ON "control"."spctl_pub_customer_subscription" ("customer_key","subscription_key")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_pub_customer_subscription_02" ON "control"."spctl_pub_customer_subscription" ("customer_key")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_pub_customer_subscription

ALTER TABLE "control"."spctl_pub_customer_subscription" ADD CONSTRAINT "pk_spclt_pub_customer_subscription" PRIMARY KEY ("subscription_key")
 USING INDEX TABLESPACE "control_tablespace"
;

ALTER TABLE "control"."spctl_pub_customer_subscription" ADD CONSTRAINT "pk_spctl_pub_customer_subscription_01" UNIQUE ("subscription_name","customer_key")
 USING INDEX TABLESPACE "control_tablespace"
;

-- Table control.spctl_pub_customer_article

CREATE TABLE "control"."spctl_pub_customer_article"(
 "customer_article_key" BigSerial NOT NULL,
 "articel_status" Character varying(20) DEFAULT 'ACTIVE' NOT NULL,
 "subscription_key" Bigint NOT NULL,
 "df_config_id" Bigint NOT NULL
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_pub_customer_article" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_pub_customer_article

CREATE INDEX "idx_spctl_pub_customer_articale01" ON "control"."spctl_pub_customer_article" ("subscription_key","customer_article_key","df_config_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_pub_customer_article_02" ON "control"."spctl_pub_customer_article" ("subscription_key")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_pub_customer_article_03" ON "control"."spctl_pub_customer_article" ("df_config_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_pub_customer_article_04" ON "control"."spctl_pub_customer_article" ("customer_article_key")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_pub_customer_article

ALTER TABLE "control"."spctl_pub_customer_article" ADD CONSTRAINT "pk_spctl_pub_customer_articale" PRIMARY KEY ("customer_article_key")
 USING INDEX TABLESPACE "control_tablespace"
;

ALTER TABLE "control"."spctl_pub_customer_article" ADD CONSTRAINT "pk_spctl_pub_customer_article_02" UNIQUE ("df_config_id","subscription_key")
 USING INDEX TABLESPACE "control_tablespace"
;

-- Table control.spctl_subscription_publication_process

CREATE TABLE "control"."spctl_subscription_publication_process"(
 "publication_process_id" BigSerial NOT NULL,
 "create_date" Timestamp DEFAULT now() NOT NULL,
 "dt_lastchange" Timestamp DEFAULT now() NOT NULL,
 "process_status" Character varying(100) DEFAULT 'WT' NOT NULL,
 "process_actribute" Text,
 "subscription_key" Bigint NOT NULL,
 "notice_id" Bigint
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;
COMMENT ON COLUMN "control"."spctl_subscription_publication_process"."process_status" IS 'WT,PS,TR,TF,SU'
;

alter table "control"."spctl_subscription_publication_process" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_subscription_publication_process

CREATE INDEX "idx_spctl_subscription_publication_process_01" ON "control"."spctl_subscription_publication_process" ("subscription_key")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_subscription_publication_process_02" ON "control"."spctl_subscription_publication_process" ("publication_process_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_subscription_publication_process_03" ON "control"."spctl_subscription_publication_process" ("notice_id")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_subscription_publication_process

ALTER TABLE "control"."spctl_subscription_publication_process" ADD CONSTRAINT "pk_spctl_subscription_publication_process" PRIMARY KEY ("publication_process_id")
;

-- Table control.spctl_subscription_publication_process_concurrent_trans

CREATE TABLE "control"."spctl_subscription_publication_process_concurrent_trans"(
 "publication_process_id" Bigint NOT NULL,
 "customer_article_key" Bigint NOT NULL,
 "export_file_name" Character varying(255),
 "export_zip_file_name" Character varying(255),
 "md5_code" Character varying(255),
 "file_size" Integer DEFAULT 0 NOT NULL,
 "is_exported" Boolean DEFAULT FALSE NOT NULL,
 "is_publicized" Boolean DEFAULT FALSE NOT NULL,
 "status" Character(5) DEFAULT 'WT' NOT NULL,
 "dt_starttime" Timestamp,
 "dt_lastchange" Timestamp DEFAULT now() NOT NULL,
 "export_process_id" Bigint,
 "transfer_process_id" Bigint,
 "error_message" Text
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;
COMMENT ON COLUMN "control"."spctl_subscription_publication_process_concurrent_trans"."file_size" IS 'export file size'
;
COMMENT ON COLUMN "control"."spctl_subscription_publication_process_concurrent_trans"."status" IS 'WT,ER,PSE,EF,TR,PST,TF,SU'
;
COMMENT ON COLUMN "control"."spctl_subscription_publication_process_concurrent_trans"."export_process_id" IS 'Process id of process export file'
;
COMMENT ON COLUMN "control"."spctl_subscription_publication_process_concurrent_trans"."transfer_process_id" IS 'process id of transfer process'
;

alter table "control"."spctl_subscription_publication_process_concurrent_trans" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_subscription_publication_process_concurrent_trans

CREATE INDEX "idx_spctl_subscription_publication_process_concurrent_trans_01" ON "control"."spctl_subscription_publication_process_concurrent_trans" ("publication_process_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_subscription_publication_process_concurrent_trans_02" ON "control"."spctl_subscription_publication_process_concurrent_trans" ("customer_article_key")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_subscription_publication_process_concurrent_trans_03" ON "control"."spctl_subscription_publication_process_concurrent_trans" ("status")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_subscription_publication_process_concurrent_trans_04" ON "control"."spctl_subscription_publication_process_concurrent_trans" ("customer_article_key","publication_process_id","status")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_subscription_publication_process_concurrent_trans

ALTER TABLE "control"."spctl_subscription_publication_process_concurrent_trans" ADD CONSTRAINT "pk_spctl_subscription_publication_process_concurrent_trans" PRIMARY KEY ("publication_process_id","customer_article_key")
;

-- Table control.spctl_notice

CREATE TABLE "control"."spctl_notice"(
 "notice_id" BigSerial NOT NULL,
 "notice_message" Character varying(255) NOT NULL
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_notice" OWNER TO warehouse_dbo
;

-- Add keys for table control.spctl_notice

ALTER TABLE "control"."spctl_notice" ADD CONSTRAINT "pk_spctl_notice" PRIMARY KEY ("notice_id")
;

-- Table control.spctl_export_module

CREATE TABLE "control"."spctl_export_module"(
 "export_module_id" BigSerial NOT NULL,
 "module_desc" Text,
 "bin_dir" Character varying(255) NOT NULL,
 "script_name" Character varying(255) NOT NULL,
 "export_dir" Character varying(255) NOT NULL,
 "export_file_name_format" Character varying(255)
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_export_module" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_export_module

CREATE INDEX "idx_spctl_export_module_01" ON "control"."spctl_export_module" ("export_module_id")
;

-- Add keys for table control.spctl_export_module

ALTER TABLE "control"."spctl_export_module" ADD CONSTRAINT "pk_spctl_export_module" PRIMARY KEY ("export_module_id")
 USING INDEX TABLESPACE "control_tablespace"
;

-- Table control.spctl_subscription_publication_checkpoint

CREATE TABLE "control"."spctl_subscription_publication_checkpoint"(
 "last_fire_time" Timestamp,
 "subscription_key" Bigint
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;

alter table "control"."spctl_subscription_publication_checkpoint" OWNER TO warehouse_dbo
;

CREATE INDEX "idx_spctl_subscription_publication_checkpoint01" ON "control"."spctl_subscription_publication_checkpoint" ()
TABLESPACE "control_tablespace"
;

-- Table control.spctl_data_source_tables

CREATE TABLE "control"."spctl_data_source_tables"(
 "data_source_table_id" BigSerial NOT NULL,
 "table_type" Character varying(20) DEFAULT 'DLA' NOT NULL,
 "table_name" Character varying(255) NOT NULL,
 "date_up_to_date" Date,
 "week_up_to_date" Character varying(100),
 "month_sine_2005_up_to_date" Integer,
 "month_up_to_date" Character varying(100),
 "checkCode" Character varying(255)
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;
COMMENT ON COLUMN "control"."spctl_data_source_tables"."table_type" IS 'DLA, MLA,DIM,FCT'
;
COMMENT ON COLUMN "control"."spctl_data_source_tables"."checkCode" IS 'SELECT md5(trim(both '' '' FROM n_tup_ins::text||n_tup_upd::text||n_tup_del::text)) FROM pg_stat_all_tables WHERE schemaname||''.''||relname=trim(both '' '' FROM ''adstraffic.daily_ad_serving_stats'') ;'
;

alter table "control"."spctl_data_source_tables" OWNER TO warehouse_dbo
;

-- Create indexes for table control.spctl_data_source_tables

CREATE INDEX "idx_spctl_data_source_tables01" ON "control"."spctl_data_source_tables" ("table_name")
;

CREATE INDEX "idx_spctl_data_source_tables02" ON "control"."spctl_data_source_tables" ("data_source_table_id")
;

-- Add keys for table control.spctl_data_source_tables

ALTER TABLE "control"."spctl_data_source_tables" ADD CONSTRAINT "pk_data_source_tables" PRIMARY KEY ("data_source_table_id")
;

ALTER TABLE "control"."spctl_data_source_tables" ADD CONSTRAINT "table_name" UNIQUE ("table_name")
;

-- Table control.spctl_customer_contact

CREATE TABLE "control"."spctl_customer_contact"(
 "customer_contact_id" BigSerial NOT NULL,
 "customer_email" Character varying(255) NOT NULL,
 "customer_phone_number" Character varying(255),
 "customer_key" Bigint,
 "customer_contact_status" Character varying(20) DEFAULT 'ACTIVE' NOT NULL
)
WITH (OIDS=FALSE)
TABLESPACE "control_tablespace"
;
COMMENT ON COLUMN "control"."spctl_customer_contact"."customer_contact_status" IS 'ACTIVE,INACTIVE'
;

-- Create indexes for table control.spctl_customer_contact

CREATE INDEX "idx_spctl_customer_contact" ON "control"."spctl_customer_contact" ("customer_contact_id")
TABLESPACE "control_tablespace"
;

CREATE INDEX "idx_spctl_customer_contact_02" ON "control"."spctl_customer_contact" ("customer_key")
TABLESPACE "control_tablespace"
;

-- Add keys for table control.spctl_customer_contact

ALTER TABLE "control"."spctl_customer_contact" ADD CONSTRAINT "pk_spctl_customer_contact" PRIMARY KEY ("customer_contact_id")
 USING INDEX TABLESPACE "control_tablespace"
;

-- Create relationships section ------------------------------------------------- 

ALTER TABLE "control"."spctl_pub_customer_article" ADD CONSTRAINT "fk_spctl_customer_sub_article" FOREIGN KEY ("subscription_key") REFERENCES "control"."spctl_pub_customer_subscription" ("subscription_key") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_pub_customer_article" ADD CONSTRAINT "fk_spctl_aticle_df_config" FOREIGN KEY ("df_config_id") REFERENCES "control"."spctl_data_file_config" ("df_config_id") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_pub_customer_subscription" ADD CONSTRAINT "fk_spctl_pub_customer_subscription" FOREIGN KEY ("customer_key") REFERENCES "control"."spctl_pub_customer" ("customer_key") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_subscription_publication_process" ADD CONSTRAINT "fk_spctl_subscription_process" FOREIGN KEY ("subscription_key") REFERENCES "control"."spctl_pub_customer_subscription" ("subscription_key") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_subscription_publication_process_concurrent_trans" ADD CONSTRAINT "pfk_spctl_subscription_process_concurrent_trans" FOREIGN KEY ("publication_process_id") REFERENCES "control"."spctl_subscription_publication_process" ("publication_process_id") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_subscription_publication_process_concurrent_trans" ADD CONSTRAINT "pfk_spctl_process_concurrent_trans_acticle" FOREIGN KEY ("customer_article_key") REFERENCES "control"."spctl_pub_customer_article" ("customer_article_key") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_subscription_publication_process" ADD CONSTRAINT "fk_subscription_process_notice" FOREIGN KEY ("notice_id") REFERENCES "control"."spctl_notice" ("notice_id") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_data_file_config" ADD CONSTRAINT "fk_data_file_config_export_module" FOREIGN KEY ("export_module_id") REFERENCES "control"."spctl_export_module" ("export_module_id") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_data_file_config" ADD CONSTRAINT "fk_spctl_data_file_config_data_source_tables" FOREIGN KEY ("data_source_table_id") REFERENCES "control"."spctl_data_source_tables" ("data_source_table_id") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_customer_contact" ADD CONSTRAINT "fk_spctl_customer_contact" FOREIGN KEY ("customer_key") REFERENCES "control"."spctl_pub_customer" ("customer_key") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_data_file_config" ADD CONSTRAINT "fk_data_sub_data_file_config" FOREIGN KEY ("data_subject_id") REFERENCES "control"."spctl_data_subject" ("data_subject_id") ON DELETE NO ACTION ON UPDATE NO ACTION
;

ALTER TABLE "control"."spctl_subscription_publication_checkpoint" ADD CONSTRAINT "fk_spctl_subscription_publication_checkpoint" FOREIGN KEY ("subscription_key") REFERENCES "control"."spctl_pub_customer_subscription" ("subscription_key") ON DELETE NO ACTION ON UPDATE NO ACTION
;


-- Grant permissions section -------------------------------------------------




