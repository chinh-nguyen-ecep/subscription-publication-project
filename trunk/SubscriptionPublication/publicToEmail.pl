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
if(@ARGV>=1){	
	main(@ARGV);
}

sub main{
	noteTime("Starting upload file to S3....");
	my ($publication_process_id)=@_;
	#Update status processs	
	my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans
				SET status=?,transfer_process_id=?,dt_lastchange=now()
				WHERE publication_process_id=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('PST',$pid,$publication_process_id);
	
	my $query ="UPDATE control.spctl_subscription_publication_process
				SET process_status=? ,dt_lastchange=now()
				WHERE publication_process_id=?
				";	
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('PST',$publication_process_id);
	sqlDisconnect($dbh);	
	note("Process id: $pid");
	#Load config data
	my $subscription_attribute=''; # from control.spctl_pub_customer_subscription.subscription_attribute
	my $process_actribute=''; # The input on each process from control.spctl_subscription_publication_process.process_actribute
	my $export_dir=''; # from control.spctl_export_module.export_dir	
	my $customer_host_name=''; # from control.spctl_pub_customer.customer_host_name
	my $customer_destination_folder=''; #from control.spctl_pub_customer.customer_destination_folder
	my $upload_file_name=''; #from control.spctl_subscription_publication_process_concurrent_trans.export_file_name
	my $file_size=0; #from control.spctl_subscription_publication_process_concurrent_trans.export_file_name file_size	
	my $zip_before_transfer='';
	my $frequence='';
	my $transfer_script_name='';
	my $folder_content_transfer_script='';
	my $customer_article_key=0;
	my $subscription_name='';
	my $subscription_desc='';
	my $query="SELECT 
			b.process_actribute
			,c.subscription_attribute
			,g.export_dir
			,h.customer_host_name
			,h.customer_destination_folder
			,a.export_file_name
			,a.file_size
			,c.frequence
			,c.zip_before_transfer
			,h.transfer_script_name
			,h.folder_content_transfer_script
			,a.customer_article_key
			,c.subscription_name
			,c.subscription_desc
		FROM
			control.spctl_subscription_publication_process_concurrent_trans a
			,control.spctl_subscription_publication_process b
			,control.spctl_pub_customer_subscription c
			,control.spctl_pub_customer_article d
			,control.spctl_data_file_config e
			,control.spctl_data_source_tables f
			,control.spctl_export_module g
			,control.spctl_pub_customer h
		WHERE
			a.publication_process_id=?
			AND a.publication_process_id=b.publication_process_id
			AND b.subscription_key=c.subscription_key
			AND a.customer_article_key=d.customer_article_key
			AND d.df_config_id=e.df_config_id
			AND e.data_source_table_id=f.data_source_table_id
			AND e.export_module_id=g.export_module_id	
			AND c.customer_key=h.customer_key
	";
	my @listFileToUpload=();
	
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute($publication_process_id);
	$query_handle->bind_columns(undef, \$process_actribute, \$subscription_attribute, \$export_dir,\$customer_host_name,\$customer_destination_folder, \$upload_file_name, \$file_size, \$frequence,\$zip_before_transfer, \$transfer_script_name, \$folder_content_transfer_script,\$customer_article_key,\$subscription_name,\$subscription_desc);
	while($query_handle->fetch()){	
		my @arrayTemp=($file_size,$upload_file_name,$customer_article_key,$customer_host_name,$customer_destination_folder);
		push(@listFileToUpload,\@arrayTemp);
	};
	sqlDisconnect($dbh);
	my $file_input='';
	note('Number files: '.@listFileToUpload);
	note("Zip before public: $zip_before_transfer");
	$i=0;
	foreach my $rows(@listFileToUpload){
		note("Upload to: $rows->[3]");
		note("Bucket: $rows->[4]");
		note("Upload file: $rows->[1]");
		note("File size: $rows->[0]");	
		note("Customer articel key: $rows->[2]");
		note("Public process id: $publication_process_id");
		if($i>0){
			$file_input=$file_input." ";
		}
		$file_input=$file_input."\"$export_dir/$rows->[1]\"";
		$i++;
	}
	#Load list email
	my $customer_email='';
	my $email_input='';
	my $query="SELECT d.customer_email
			FROM control.spctl_subscription_publication_process a
			,control.spctl_pub_customer_subscription b
			,control.spctl_pub_customer c
			,control.spctl_customer_contact d
			WHERE a.subscription_key=b.subscription_key
			AND b.customer_key=c.customer_key
			AND d.customer_key=c.customer_key
			AND a.publication_process_id=?
			AND d.customer_contact_status='ACTIVE'
			";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute($publication_process_id);
	$query_handle->bind_columns(undef, \$customer_email);
	$i=0;
	while($query_handle->fetch()){	
		if($i>0){
			$email_input=$email_input.",";
		}
		$email_input=$email_input.$customer_email;
		$i++;
	};	
	
	#note("Files input: $file_input");
	#note("Emails input: $email_input");
	
	my $email_subject='"'.$subscription_name.'"';
	my $email_content="\"$subscription_desc <p/>The email send auto by Subscription publication system. Do not reply this email.\"";
	my @cmd=`cd $folder_content_transfer_script && java -jar emailReport.jar $email_subject $email_content $email_input $file_input 2>log/$pid`;
	my @errorOutput=fileToArray("log/$pid");
	note("Standar output: @cmd");
	note("Error output: @errorOutput");
	my $comandOutputLastLine=lastLine(@cmd);	
	if($comandOutputLastLine eq 'Send report to email - Done' && @errorOutput==0){
		my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans
					SET status=? ,is_publicized=TRUE,dt_lastchange=now(),error_message=null
					WHERE publication_process_id=?
					";
		my $dbh=getConnection();
		my $query_handle = $dbh->prepare($query);
		$query_handle->execute('SU',$publication_process_id);			
		my $query ="UPDATE control.spctl_subscription_publication_process
					SET process_status=? ,dt_lastchange=now()
					WHERE publication_process_id=?
					";		
		my $query_handle = $dbh->prepare($query);
		$query_handle->execute('SU',$publication_process_id);
		sqlDisconnect($dbh);	
		noteTime("Email completed...");
	}else{
		my $errorMess=arrayToString(@errorOutput);
		my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans
					SET status=?,error_message=?,dt_lastchange=now()
					WHERE publication_process_id=?
					";
		my $dbh=getConnection();
		my $query_handle = $dbh->prepare($query);
		$query_handle->execute('TF',$errorMess,$publication_process_id);
		
		my $query ="UPDATE control.spctl_subscription_publication_process
					SET process_status=? ,dt_lastchange=now()
					WHERE publication_process_id=?
					";
		my $query_handle = $dbh->prepare($query);
		$query_handle->execute('TF',$publication_process_id);
		sqlDisconnect($dbh);
		noteTime("Email completed with some thing wrong...");
		noticeErrorToEmail("Public to email Fail with <b>process id</b>: $publication_process_id <p/>Error: <br/>$errorMess");			
	}
	
	# remove error lof file
	system("rm -rf log/$pid");


	
}

