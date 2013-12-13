use Digest::MD5;
$pid=$$;
my $database='warehouse';
my $mode='null';
my $table_name='null';
my $start_date='null';
my $end_date='null';
my $calendar_year_month='null';
my $year_week='null';
my $md5code='null';
my $file_unzip='';
my $delete_count=0;
my $import_count=0;
my($data_file_id,$file_name,$import_dir,$success_dir,$error_dir);
if(@ARGV>=5){	
	main(@ARGV);
}

sub main{
	($data_file_id,$file_name,$import_dir,$success_dir,$error_dir)=@_;
	updateStatusProcessing();
	processFileNameInput($file_name);
	unzip();
	checkMd5CodeFromUzipFile();
	import();	
	deleteUnzipFile();
	updateImportCount();
}
sub updateStatusProcessing{
	system("psql -d $database -c \"UPDATE control.data_file SET file_status='PS',dt_file_extracted=now(),dt_process_transformed=now() WHERE data_file_id=$data_file_id\" ");
}
sub processFileNameInput{
	my $file_name=shift;
	print "File name input: $file_name \n";
	my @values 	=split('\.', $file_name);
	$mode=$values[0];
	print "Mode: $mode\n";
	if($mode eq 'daily'){
		$table_name="$values[1].$values[2]";
		$start_date="$values[3]";
		$end_date=$start_date;
		$md5code="$values[4]";
	}elsif($mode eq 'date_range'){
		$table_name="$values[1].$values[2]";
		$start_date="$values[3]";
		$end_date="$values[4]";
		$md5code="$values[5]";
	}elsif($mode eq 'weekly'){
		$table_name="$values[1].$values[2]";
		$year_week="$values[3]";		
		$md5code="$values[4]";
	}elsif($mode eq 'monthly'){
		$table_name="$values[1].$values[2]";
		$calendar_year_month="$values[3]";		
		$md5code="$values[4]";
	}else{		
		die "Can get import mode! $import_dir/$file_name";
	}
	print "Mode: $mode \n Table name: $table_name \n Start date: $start_date \n End date: $end_date \n Year week: $year_week \n Month: $calendar_year_month \n Md5 code: $md5code \n";
}

sub unzip{
	my $unzip_cmd='';
	$unzip_cmd="cd $import_dir && unzip -o $file_name";
	print "Unzip cmd: $unzip_cmd\n";
	my @cmd=`$unzip_cmd 2>$$.log` ;
	my @error=`cat $import_dir/$$.log`;
	my $cmd=`rm -rf $import_dir/$$.log`;	
	if(@error>0){
		die @error;
	}else{
		$file_unzip=$file_name;
		$file_unzip=~ s/\.zip//g;
		print "Unzip file: $import_dir$file_unzip \n";	
	}
};

sub checkMd5CodeFromUzipFile{
	my $unzipFileMd5code=md5sum("$import_dir$file_unzip");
	print "UnzipFileMd5code: $unzipFileMd5code \n Server md5 code: $md5code \n";
	if($unzipFileMd5code eq $md5code){
		print "Check md5 is match. \n";
	}else{		
		system("psql -d $database -c \"UPDATE control.data_file SET file_status='EF' WHERE data_file_id=$data_file_id\" ");
		die "md5 code is not match!";
	}
}

sub import{
	my $delete_cmd='';
	my $import_cmd='';
	if($mode eq 'daily'){
		$delete_cmd="psql -d $database -c \"DELETE FROM $table_name WHERE full_date='$end_date' \" ";
		$import_cmd="psql -d $database -c \"COPY $table_name FROM '$import_dir/$file_unzip' WITH DELIMITER '|' \" ";
	}elsif($mode eq 'date_range'){
		$delete_cmd="psql -d $database -c \"DELETE FROM $table_name WHERE full_date BETWEEN '$start_date' AND '$end_date' \" ";
		$import_cmd="psql -d $database -c \"COPY $table_name FROM '$import_dir/$file_unzip' WITH DELIMITER '|' \" ";
	}elsif($mode eq 'weekly'){
		$delete_cmd="psql -d $database -c \"DELETE FROM $table_name WHERE year_week= '$year_week' \" ";
		$import_cmd="psql -d $database -c \"COPY $table_name FROM '$import_dir/$file_unzip' WITH DELIMITER '|' \" ";
	}elsif($mode eq 'monthly'){
		$delete_cmd="psql -d $database -c \"DELETE FROM $table_name WHERE calendar_year_month= '$calendar_year_month' \" ";
		$import_cmd="psql -d $database -c \"COPY $table_name FROM '$import_dir/$file_unzip' WITH DELIMITER '|' \" ";
	}	
	print "Delete cmd: $delete_cmd\n";
	print "Import cmd: $import_cmd\n";
	my @cmd=`$delete_cmd`;
	print @cmd;	
	my $cmd=`$import_cmd 2>$$.log`;	
	my @values_cmd=split(' ', $cmd);
	$import_count=$values_cmd[1];
	
	my @error=`cat $$.log`;
	$cmd=`rm -rf $$.log`;	
	$countError=@error;
	if($countError>0){
		deleteUnzipFile();
		die "Import error: @error";
	}else{
		print "Import count: $import_count \n";
	}
	
}

sub deleteUnzipFile{
	my $deleteUnzip_cmd="rm -rf $import_dir$file_unzip";
	print "Delete unzip file cmd: $deleteUnzip_cmd \n";
	system($deleteUnzip_cmd);
}

sub updateImportCount{
	system("psql -d $database -c \"UPDATE control.data_file SET dt_process_loaded=now(), staging_load_count= $import_count, fact_table_load_count= $import_count WHERE data_file_id=$data_file_id\" ");
}

sub verifyImportCount{
	my $cmd="";
	if($mode eq 'daily'){
		$cmd="psql -d $database -c \"SELECT COUNT(1) FROM $table_name WHERE full_date='$end_date' \" ";		
	}elsif($mode eq 'date_range'){
		$cmd="psql -d $database -c \"SELECT COUNT(1) FROM $table_name WHERE full_date BETWEEN '$start_date' AND '$end_date' \" ";		
	}elsif($mode eq 'weekly'){
		$cmd="psql -d $database -c \"SELECT COUNT(1) FROM $table_name WHERE year_week= '$year_week' \" ";		
	}elsif($mode eq 'monthly'){
		$cmd="psql -d $database -c \"SELECT COUNT(1) FROM $table_name WHERE calendar_year_month= '$calendar_year_month' \" ";
	}	
	
}

sub md5sum{
  my $file = shift;
  my $digest = "";
  eval{
    open(FILE, $file) or die "Can't find file $file\n";
    my $ctx = Digest::MD5->new;
    $ctx->addfile(*FILE);
    $digest = $ctx->hexdigest;
    close(FILE);
  };
  if($@){
    print $@;
    return "";
  }
  return $digest;
}