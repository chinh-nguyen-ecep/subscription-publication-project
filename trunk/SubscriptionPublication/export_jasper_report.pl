use DBI;
use Date::Pcalc qw(:all);
use POSIX qw(strftime);
use DBIx::AutoReconnect;
use Digest::MD5;

require "config/config.pl";
require "utils/connectionDB.pl";
require "utils/md5_checksum.pl";
require "utils/utils.pl";

$pid=$$;
$error_count=0;
$error_mess='';
if(@ARGV>=2){	
	main(@ARGV);
}

sub main{
	noteTime("Starting export process....");
	my ($publication_process_id,$customer_article_key)=@_;
	############################################################################################
	############ Update status processs to PSE--Processing export . Enforcement! ###############
	############################################################################################
	my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans 
				SET status=? , export_process_id=?, dt_starttime=now(),dt_lastchange=now()
				WHERE publication_process_id=? AND customer_article_key=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('PSE',$pid,$publication_process_id,$customer_article_key);
	sqlDisconnect($dbh);	
	note("Process id: $pid");
	note("Group id: $) - $( -$< - $>");
	#Load config data
	my $subscription_attribute=''; # from control.spctl_pub_customer_subscription.subscription_attribute
	my $process_actribute=''; # The input on each process from control.spctl_subscription_publication_process.process_actribute
	
	my $export_table_name=''; # from control.spctl_data_source_tables.table_name
	my $table_type=''; # from control.spctl_data_source_tables.table_type
	my $report_date_vailable=''; # from control.spctl_data_source_tables.date_up_to_date
	
	my $export_file_format=''; # from control.spctl_data_file_config.df_config_format
	my $export_file_name_format=''; # from control.spctl_export_module.export_file_name_format
	my $export_dir=''; # from control.spctl_export_module.export_dir
	my $df_source_file='';
	my $df_attribute='';
	my $report_week='';
	my $report_month_since_2005='';
	my $report_month='';
	my $report_date_sk=0;
	my $report_name='';
	my $query="SELECT 
			b.process_actribute
			,c.subscription_attribute
			,e.df_config_format as export_file_format
			,f.table_name as export_table_name
			,f.table_type
			,h.full_date as report_date
			,g.export_dir
			,g.export_file_name_format
			,e.df_source_file
			,e.df_attribute
			,f.week_up_to_date as report_week
			,f.month_sine_2005_up_to_date as report_month_since_2005
			,f.month_up_to_date as report_month
			,h.date_sk as report_date_sk
			,e.df_config_name as report_name
		FROM
			control.spctl_subscription_publication_process_concurrent_trans a
			,control.spctl_subscription_publication_process b
			,control.spctl_pub_customer_subscription c
			,control.spctl_pub_customer_article d
			,control.spctl_data_file_config e
			,control.spctl_data_source_tables f
			,control.spctl_export_module g
			,refer.date_dim h
		WHERE
			a.publication_process_id=?
			AND a.customer_article_key=?
			AND a.publication_process_id=b.publication_process_id
			AND b.subscription_key=c.subscription_key
			AND a.customer_article_key=d.customer_article_key
			AND d.df_config_id=e.df_config_id
			AND e.data_source_table_id=f.data_source_table_id
			AND e.export_module_id=g.export_module_id	
			AND f.date_up_to_date=h.full_date
	";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute($publication_process_id,$customer_article_key);
	$query_handle->bind_columns(undef, \$process_actribute, \$subscription_attribute, \$export_file_format, \$export_table_name, \$table_type, \$report_date_vailable, \$export_dir,\$export_file_name_format,\$df_source_file,\$df_attribute,\$report_week,\$report_month_since_2005,\$report_month,\$report_date_sk,\$report_name);
	$query_handle->fetch();
	sqlDisconnect($dbh);
	my $report_date=$report_date_vailable;

	#Paser df_attribute to get rollback date and process input parameter
	my $roll_back_date=0;
	my $v_start_date='';
	my $v_start_date_sk=0;
	my @string_as_array = split( ' ', $df_attribute );
	foreach $rows(@string_as_array){
		my @temp_array=split( '=', $rows );
		my $key=$temp_array[0];
		my $value=$temp_array[1];
		
		if($key eq 'roll_back_date'){
			$roll_back_date=$value
		}
	}
	my $query="SELECT full_date,date_sk FROM refer.date_dim WHERE date_sk=$report_date_sk-$roll_back_date";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef,\$v_start_date,\$v_start_date_sk);
	$query_handle->fetch();
	sqlDisconnect($dbh);
  #Paser subscription_attribute to got date report
  # The format of subscription_attribute should be: 
  #		date=2014-02-10
  #		start_date=2014-02-01&end_date=2014-01-02 
  #		week=2014-W109
  #		month=2014-Jan
	if($process_actribute ne ''){
		my @string_as_array = split( '&', $process_actribute );
		foreach $rows(@string_as_array){
			my @temp_array=split( '=', $rows );
			my $key=$temp_array[0];
			my $value=$temp_array[1];
			
			if($key eq 'date' || $key eq 'end_date'){
					$report_date=$value;
					my $query="SELECT date_sk FROM refer.date_dim WHERE full_date=?";
					my $dbh=getConnection();
					my $query_handle = $dbh->prepare($query);
					$query_handle->execute($report_date);
					$query_handle->bind_columns(undef,\$report_date_sk);
					$query_handle->fetch();
					# reload $v_start_date,$v_start_date_sk when $report_date_sk changed
						if($v_start_date eq ''){
							$query="SELECT full_date,date_sk FROM refer.date_dim WHERE date_sk=$report_date_sk-$roll_back_date";
							$query_handle = $dbh->prepare($query);
							$query_handle->execute();
							$query_handle->bind_columns(undef,\$v_start_date,\$v_start_date_sk);
							$query_handle->fetch();
						}
						
					sqlDisconnect($dbh);
			}
			if($key eq 'start_date'){
					$v_start_date=$value;
					my $query="SELECT date_sk FROM refer.date_dim WHERE full_date=?";
					my $dbh=getConnection();
					my $query_handle = $dbh->prepare($query);
					$query_handle->execute($v_start_date);
					$query_handle->bind_columns(undef,\$v_start_date_sk);
					$query_handle->fetch();
					sqlDisconnect($dbh);
			}
			if($key eq 'week'){
					$report_week=$value;					
			}
			if($key eq 'month'){
					$report_month=$value;	
					my $query="SELECT month_since_2005 FROM refer.month_dim WHERE calendar_year_month=?";
					my $dbh=getConnection();
					my $query_handle = $dbh->prepare($query);
					$query_handle->execute($report_month);
					$query_handle->bind_columns(undef,\$report_month_since_2005);
					$query_handle->fetch();
					sqlDisconnect($dbh);					
			}
		}
	}	
	$df_attribute=~ s/\{v_eastern_date_sk\}/$report_date_sk/g;
	$df_attribute=~ s/\{v_start_date_sk\}/$v_start_date_sk/g;
	$df_attribute=~ s/\{full_date\}/$report_date/g;
	$df_attribute=~ s/\{v_start_full_date\}/$v_start_date/g;
	$df_attribute=~ s/\{year_week\}/$report_week/g;
	$df_attribute=~ s/\{calendar_year_month\}/$report_month/g;
	$df_attribute=~ s/\{month_since_2005\}/$report_month_since_2005/g;
	
	#Process jasper source file
	my $source_dir='';
	my $source_file_name='';
	my @temp_array=split( '/', $df_source_file );
	$source_file_name=lastLine(@temp_array);
	$source_file_name=~ s/\.jrxml//;
	for($i=1;$i<@temp_array-1;$i++){
		$source_dir=$source_dir.'/'.$temp_array[$i];
	}
	############################
	## Export jasper report to file ####
	############################
	
	note("Report name: $report_name");
	note("Jasper source: $df_source_file");
	note("Source dir: $source_dir");
	note("Source file name: $source_file_name");
	note("Export format: $export_file_format");
	note("Attribute: $df_attribute");
	
	my $file_name_temp="jasper_export_temp.$time_tmp";
	my $file_size=0;
	my $final_file_name='';	
	my $md5value='';
	my @cmd=`cd $bin_dir && java -jar jasperReportGenerater.jar "$source_dir" "$source_file_name" "$export_dir" $export_file_format $file_name_temp $df_attribute 2>log/$pid`;	
	my @errorOutput=fileToArray("log/$pid");
	#find exception in errorOutPut
	foreach $rows(@errorOutput){
		if(index($rows, 'Exception')>-1 || index($rows, 'exception')>-1){
			$error_count++;
		}
	}
	if($error_count>0){
		$error_count++;
		$error_mess=arrayToString(@errorOutput);
		#print @errorOutput;
	}else{
		$file_name_temp=$file_name_temp.'.'.$export_file_format;
		my $md5value=md5sum("$export_dir/$file_name_temp");
		note("Md5sum: $md5value");
		##Change name of file
		## file format "schema.table_name.date.md5.timecode.csv"
		$final_file_name=$export_file_name_format;
		$final_file_name=~ s/reportName/$report_name/g;
		$final_file_name=~ s/md5/$md5value/g;
		$final_file_name=~ s/timecode/$time_tmp/g;
		$final_file_name=~ s/format/$export_file_format/g;
		
		
		if($table_type eq 'MLA'){
			$final_file_name=~ s/date/$report_month/g;
		}elsif($table_type eq 'WLA'){
			$final_file_name=~ s/date/$report_week/g;
		}elsif($table_type eq 'DLA'){
			if($roll_back_date>0){
				$final_file_name=~ s/date/$v_start_date.$report_date/g;
			}
		}else{
				$final_file_name=~ s/date/$report_date/g;
			}
		
		
		$cmd=`mv "$export_dir/$file_name_temp" "$export_dir/$final_file_name"`;
		my $file_size_t=`du -k "$export_dir/$final_file_name"`;
		($file_size)= $file_size_t=~ /(\d+)/;
		note("Exported file name: $final_file_name");
		note("Exported file size: $file_size K");
		note("Exported dir: $export_dir");	
	}

	
	########################################################################################
	# Update control.spctl_subscription_publication_process_concurrent_trans. Enforcement!##
	########################################################################################
	if($error_count>0){
		my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans 
					SET is_exported=FALSE, status='EF',error_message=?,dt_lastchange=now()
					WHERE publication_process_id=? AND customer_article_key=?
					";
		my $dbh=getConnection();
		my $query_handle = $dbh->prepare($query);
		$query_handle->execute($error_mess,$publication_process_id,$customer_article_key);
		sqlDisconnect($dbh);
		noteTime("Export completed with something wrong...");
		noticeErrorToEmail("Export file error with <b>process id</b>: $publication_process_id <b>article key</b>: $customer_article_key <p/> $) - $( -$< - $> - Error: <br/>$error_mess");				
	}else{
		my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans 
					SET export_file_name=? , md5_code=?, is_exported=TRUE, file_size=?, status='TR',error_message=null,dt_lastchange=now()
					WHERE publication_process_id=? AND customer_article_key=?
					";
		my $dbh=getConnection();
		my $query_handle = $dbh->prepare($query);
		$query_handle->execute($final_file_name,$md5value,$file_size,$publication_process_id,$customer_article_key);
		sqlDisconnect($dbh);
		noteTime("Export completed...");	
	}

	# remove error lof file
	system("rm -rf log/$pid");
}