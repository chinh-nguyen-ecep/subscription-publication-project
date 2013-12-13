
@cmd=`psql -d analyticsdb -c "SELECT file_name FROM control.data_file WHERE file_status='SU' AND dt_file_queued::date>=now()::date-10 ORDER BY data_file_id desc --LIMIT 10"`;
#@cmd=`psql -d analyticsdb -c "SELECT file_name FROM control.data_file WHERE file_status='SU'"`;
foreach (@cmd) { 	
	my $my_file=trim($_);
	#print "My file: $my_file\n"	;
	if(length($my_file)>0 ){
		my $file_size=`du -k download/$my_file`;
		my($_file_size) = $file_size=~ /(\d+)/;
		$file_size=$_file_size;	
		if($file_size>0 ){
			print "Clear up file: $my_file\n";
			print "File size: $file_size\n";
			my $cmd="> download/$my_file";
			print $cmd."\n";
			system($cmd);
		}	
	}
	
 } 
 
 sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}
 
