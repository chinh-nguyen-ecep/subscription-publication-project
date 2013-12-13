use DBI;
use Date::Pcalc qw(:all);
use POSIX qw(strftime);
use DBIx::AutoReconnect;
use Digest::MD5;

require "config/config.pl";
require "utils/connectionDB.pl";
require "utils/md5_checksum.pl";
require "utils/utils.pl";

my $pid=$$;
my $subscription_attribute=''; # from control.spctl_pub_customer_subscription.subscription_attribute
my $process_actribute=''; # The input on each process from control.spctl_subscription_publication_process.process_actribute

my $export_table_name=''; # from control.spctl_data_source_tables.table_name
my $table_type=''; # from control.spctl_data_source_tables.table_type
my $report_date_vailable=''; # from control.spctl_data_source_tables.date_up_to_date

my $export_file_format=''; # from control.spctl_data_file_config.df_config_format
my $export_file_name_format=''; # from control.spctl_export_module.export_file_name_format
my $export_dir=''; # from control.spctl_export_module.export_dir

my $mode='daily';
my $report_date='';
my $start_date='';
my $end_date='';
my $calendar_year_month='';
my $year_week='';

my $final_file_name='';
my $file_size=0;
my $md5value='';
my ($publication_process_id,$customer_article_key);
if(@ARGV>=2){
	($publication_process_id,$customer_article_key)=@ARGV;
	main();
}

sub main{
	noteTime("Starting export process....");
	
	############################################################################################
	############ Update status processs to PSE--Processing export . Enforcement! ###############
	############################################################################################	
	runPSQL("UPDATE control.spctl_subscription_publication_process_concurrent_trans SET status='PSE' , export_process_id=$pid,  dt_starttime=now(), dt_lastchange=now() WHERE publication_process_id=$publication_process_id AND customer_article_key=$customer_article_key");
	note("Process id: $pid");
	#Load config data

	loadExportInput();
	
	############################
	## Export table to file ####
	############################
	export();
	
	########################################################################################
	# Update control.spctl_subscription_publication_process_concurrent_trans. Enforcement!##
	########################################################################################

		runPSQL("UPDATE control.spctl_subscription_publication_process_concurrent_trans SET export_file_name='$final_file_name' , md5_code='$md5value', is_exported=TRUE, file_size=$file_size, status='TR',dt_lastchange=now() WHERE publication_process_id=$publication_process_id AND customer_article_key=$customer_article_key");	

		noteTime("Export completed...");
	
}

sub loadExportInput{
	my $query="SELECT 
			b.process_actribute
			,c.subscription_attribute
			,e.df_config_format as export_file_format
			,f.table_name as export_table_name
			,f.table_type
			,b.create_date::date-1 as report_date
			,g.export_dir
			,g.export_file_name_format
			,f.month_up_to_date
			,f.week_up_to_date
		FROM
			control.spctl_subscription_publication_process_concurrent_trans a
			,control.spctl_subscription_publication_process b
			,control.spctl_pub_customer_subscription c
			,control.spctl_pub_customer_article d
			,control.spctl_data_file_config e
			,control.spctl_data_source_tables f
			,control.spctl_export_module g
		WHERE
			a.publication_process_id=?
			AND a.customer_article_key=?
			AND a.publication_process_id=b.publication_process_id
			AND b.subscription_key=c.subscription_key
			AND a.customer_article_key=d.customer_article_key
			AND d.df_config_id=e.df_config_id
			AND e.data_source_table_id=f.data_source_table_id
			AND e.export_module_id=g.export_module_id	
	";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute($publication_process_id,$customer_article_key);
	$query_handle->bind_columns(undef, \$process_actribute, \$subscription_attribute, \$export_file_format, \$export_table_name, \$table_type, \$report_date_vailable, \$export_dir,\$export_file_name_format,\$calendar_year_month,\$year_week);
	$query_handle->fetch();
	sqlDisconnect($dbh);
	note("Process input: $process_actribute");
	#Get mode export from data source
	if($table_type eq 'DLA'){
			$mode='daily';		
			$report_date=$report_date_vailable;
		}elsif($table_type eq 'MLA'){
			$mode='monthly'
		}elsif($table_type eq 'WLA'){
			$mode='weekly'
		}else{
			die "We can get export mode $mode. Data source are not in type DLA MLA WLA";
		}
	if(trim($process_actribute) eq ""){
	# No process actribute input. we will get default value from data source table		
		
	}else{
	# Process process actribute input 
	# Example: "date=2013-06-13"
	# Example: "start_date=2013-06-10&end_date=2013-06-13"
	# Example: "calendar_year_month=2013-Mar"
	# Example: "year_week=2013-W23"
		my @process_actribute_array=split( '&', trim($process_actribute) );
		foreach $row(@process_actribute_array){
			my @temp_array=split( '=', $row );
			my $key=$temp_array[0];
			my $value=$temp_array[1];
			if($key eq "mode"){
				$mode=$value;
			}elsif($key eq "date"){
				$report_date=$value;
			}elsif($key eq "start_date"){
				$start_date=$value;
			}elsif($key eq "end_date"){
				$end_date=$value;
				$report_date=$value;
			}elsif($key eq "calendar_year_month"){
				$calendar_year_month=$value;
			}elsif($key eq "year_week"){
				$year_week=$value;
			}else{
				die "Wrong input param $key! date - start_date - end_date - calendar_year_month - year_week";
			}
		}
		if($start_date ne '' && $end_date ne ''){
		$mode='date_range';
		}
	}	

	#Check input
	if($mode eq 'daily' && $report_date eq ''){
		$report_date=$report_date_vailable
	}elsif($mode eq 'date_range' && ($start_date eq '' || $end_date eq '')){
		die "Can not get report date: wrong input syntax! Example: start_date=2013-06-01 end_date=2013-06-02";
	}elsif($mode eq 'weekly' && $year_week eq ''){
		die "Can not get report date: wrong input syntax! Example: year_week=2013-W23";
	}elsif($mode eq 'monthly' && $calendar_year_month eq ''){
		die "Can not get report date: wrong input syntax! Example: calendar_year_month=2013-Apr";
	}
	

	note("Export table: $export_table_name");
	note("Export mode: $mode");
	note("Export date: $report_date");
	note("Export start date: $start_date");
	note("Export end date: $end_date");
	note("Export week: $year_week");
	note("Export month: $calendar_year_month");
}

sub export{
	my $file_name_temp="$export_table_name.$report_date.md5.$time_tmp.csv";
	## file format "export_mode.table_name.export_date.md5.timecode.csv"
	$final_file_name=$export_file_name_format;
	
	$final_file_name=~ s/table_name/$export_table_name/g;	
	$final_file_name=~ s/timecode/$time_tmp/g;
	
	my $exportQuery="";
	if($mode eq 'daily'){
		$exportQuery="COPY (SELECT * FROM $export_table_name WHERE full_date='$report_date' AND is_active=true) TO '$export_dir/$file_name_temp' WITH DELIMITER '|'";
		$final_file_name=~ s/export_date/$report_date/g; #Set date value to file name		
	}elsif($mode eq 'date_range'){
		$exportQuery="COPY (SELECT * FROM $export_table_name WHERE full_date BETWEEN '$start_date' AND '$end_date' AND is_active=true) TO '$export_dir/$file_name_temp' WITH DELIMITER '|'";
		$final_file_name=~ s/export_date/$start_date\.$end_date/g; #Set date value to file name
	}elsif($mode eq 'monthly'){
		$exportQuery="COPY (SELECT * FROM $export_table_name WHERE calendar_year_month='$calendar_year_month' AND is_active=true) TO '$export_dir/$file_name_temp' WITH DELIMITER '|'";
		$final_file_name=~ s/export_date/$calendar_year_month/g; #Set date value to file name
	}elsif($mode eq 'weekly'){
		$exportQuery="COPY (SELECT * FROM $export_table_name WHERE year_week='$year_week' AND is_active=true) TO '$export_dir/$file_name_temp' WITH DELIMITER '|'";
		$final_file_name=~ s/export_date/$year_week/g; #Set date value to file name
	}
	
	%resultCMD=runPSQL($exportQuery);
	@cmd=@{$resultCMD{'stout'}};
	@error=@{$resultCMD{'erout'}};
	my $countErrorOut=@error;
	if($countErrorOut>0){		
		die "Export error: \n @error";		
	}else{
		$md5value=md5sum("$export_dir/$file_name_temp");	
		$final_file_name=~ s/md5/$md5value/g; #Set md5 value to file name
		$final_file_name=~ s/export_mode/$mode/g;	#Set mode value to file name
		note("Md5sum: $md5value");
		##Change export file name to final
		$cmd=`mv $export_dir/$file_name_temp $export_dir/$final_file_name`;
		$file_size=`du -k $export_dir/$final_file_name`;
		my($_file_size) = $file_size=~ /(\d+)/;
		$file_size=$_file_size;
		note("Exported file name: $final_file_name");
		note("Exported file size: $file_size K");
		note("Exported dir: $export_dir");	
		note("Exported: @cmd");
	}

}


