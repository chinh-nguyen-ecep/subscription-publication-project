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
$publication_process_id=0;
$errorMess='';
$zip_file_name='';
@cmd=();
@errorOutput=();
if(@ARGV>=1){	
	main(@ARGV);
}

sub main{
	noteTime("Starting upload file to S3....");
	($publication_process_id)=@_;
	############################################################################################
	############ Update status processs to PST--Processing transfer . Enforcement! ##########
	############################################################################################	
	updateStatusBeginProcessing();
	note("Process id: $pid");
	############################################################################################
	## Load config data! ############
	############################################################################################
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
		ORDER BY a.customer_article_key
	";
	my @listFileToUpload=();	
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute($publication_process_id);
	$query_handle->bind_columns(undef, \$process_actribute, \$subscription_attribute, \$export_dir,\$customer_host_name,\$customer_destination_folder, \$upload_file_name, \$file_size, \$frequence,\$zip_before_transfer, \$transfer_script_name, \$folder_content_transfer_script,\$customer_article_key);
	while($query_handle->fetch()){	
		my @arrayTemp=($file_size,$upload_file_name,$customer_article_key,$customer_host_name,$customer_destination_folder);
		push(@listFileToUpload,\@arrayTemp);
	};
	sqlDisconnect($dbh);
	
	#######################################################
	############ Begining process transfer! ###############
	#######################################################
	print 'Number files will be uploaded: '.@listFileToUpload."\n";
		foreach my $rows(@listFileToUpload){
			note("Upload to: $rows->[3]");
			note("Bucket: $rows->[4]");
			note("Upload file: $rows->[1]");
			note("File size: $rows->[0]");	
			note("Zip before transfer: $zip_before_transfer");	
			note("Customer articel key: $rows->[2]");
			note("Public process id: $publication_process_id");
			#Update status in process trans
			updateTransStatusBeginProcess($rows->[2]);

			# transfer process
			#Zip file before transfer
			$zip_file_name=$rows->[1].".zip";
			if($zip_before_transfer==1){
				@cmd=`cd $export_dir && zip -r $zip_file_name $rows->[1] 2>$bin_dir/log/$pid`;							
				@errorOutput=fileToArray("$bin_dir/log/$pid");							
				if(@errorOutput>0){
					note("Zip file error...");
					$error_count++;
					$errorMess=arrayToString(@errorOutput);
					#Update status in process trans
					updateTransStatusError($rows->[2]);					
				}else{
					note("Zip file OK.....");
					@cmd=`cd $folder_content_transfer_script && java -jar S3Uploader.jar $customer_destination_folder $export_dir/$zip_file_name 2>log/$pid`;					
				}
			}else{
				$zip_file_name='';
				@cmd=`cd $folder_content_transfer_script && java -jar S3Uploader.jar $customer_destination_folder $export_dir/$rows->[1] 2>log/$pid`;
			}		
			
			@errorOutput=fileToArray("$bin_dir/log/$pid");			
			#note("Standar output: @cmd");
			note("Error output: @errorOutput");
			my $comandOutputLastLine=lastLine(@cmd);	
			#print "Line numbers: ".$lineNumberOutPut."\n";
			if($comandOutputLastLine == '-Done.'){
				note('Upload Ok...');
				updateTransStatusSucessfull($rows->[2]);
			}else{
				note("Upload error...");
				$error_count++;
				$errorMess=arrayToString(@errorOutput);
				#Update status in process trans
				updateTransStatusError($rows->[2]);
			}	
		}
	########################################################################################
	# Update control.spctl_subscription_publication_process.process_status. TF if error ELSE SU. Enforcement!##
	########################################################################################		
		if($error_count>0){
			updateStatusError();
		}else{
			updateStatusSucessfull();
		}

	# remove error lof file
	system("rm -rf log/$pid");



}

sub updateStatusBeginProcessing{
	my $query ="UPDATE control.spctl_subscription_publication_process
				SET process_status=?,dt_lastchange=now()
				WHERE publication_process_id=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('PST',$publication_process_id);
	sqlDisconnect($dbh);	
}

sub updateStatusSucessfull{
	my $query ="UPDATE control.spctl_subscription_publication_process
				SET process_status=?,dt_lastchange=now()
				WHERE publication_process_id=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('SU',$publication_process_id);
	sqlDisconnect($dbh);	
	noteTime("Upload completed...");
}

sub updateStatusError{
	my $query ="UPDATE control.spctl_subscription_publication_process
				SET process_status=? ,dt_lastchange=now()
				WHERE publication_process_id=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('TF',$publication_process_id);
	sqlDisconnect($dbh);
	noteTime("Upload completed with something wrong...");
	noticeErrorToEmail("Public Fail with <b>process id</b>: $publication_process_id");
}


sub updateTransStatusSucessfull{
	my $customer_article_key=shift;
	#Update status in process trans
	my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans
				SET status=?,is_publicized=TRUE,export_zip_file_name=?,error_message=null,dt_lastchange=now()
				WHERE publication_process_id=? AND customer_article_key=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('SU',$zip_file_name,$publication_process_id,$customer_article_key);
	sqlDisconnect($dbh);
}
sub updateTransStatusError{
	my $customer_article_key=shift;
	my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans
				SET status=? ,error_message=?,dt_lastchange=now(),is_publicized=true
				WHERE publication_process_id=? AND customer_article_key=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('TF',$errorMess,$publication_process_id,$customer_article_key);
	sqlDisconnect($dbh);
	noticeErrorToEmail("Public Fail with <b>process:</b> $publication_process_id. At <b>Article key</b>: $customer_article_key");	
}

sub updateTransStatusBeginProcess{
	my $customer_article_key=shift;
	my $query ="UPDATE control.spctl_subscription_publication_process_concurrent_trans
				SET status=?,transfer_process_id=?,dt_lastchange=now()
				WHERE publication_process_id=? AND customer_article_key=?
				";
	my $dbh=getConnection();
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute('PST',$pid,$publication_process_id,$customer_article_key);
	sqlDisconnect($dbh);	
}

