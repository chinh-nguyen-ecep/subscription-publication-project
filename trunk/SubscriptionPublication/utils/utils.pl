sub noteTime{
	my $text=shift;
	my ($sec,$min,$hour,$day,$month,$yr19,@rest) =   localtime(time);
	$month++;
	$yr19+=1900;
	print "# $day-$month-$yr19 $hour:$min:$sec : $text\n";
	#sleep(2);
}
sub note{
	my $text=shift;
	print "*$text\n";
	#sleep(2);
}

# Perl trim function to remove whitespace from the start and end of the string
sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}
# Left trim function to remove leading whitespace
sub ltrim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	return $string;
}
# Right trim function to remove trailing whitespace
sub rtrim($)
{
	my $string = shift;
	$string =~ s/\s+$//;
	return $string;
}

sub lastLine{
	@arrayInput=@_;
	
	my $numberOfArray=@arrayInput;
	
	return trim(@arrayInput[$numberOfArray-1]);
}

sub fileToArray{
	my $file=shift;	
	my @cmd=`cat $file`;
}
sub arrayToString{
	my @arrayInput=@_;
	my $result='';
	foreach $temp(@arrayInput){
		$result=$result.$temp."\n";
	}
	return $result;
}
sub noticeErrorToEmail{
	my $mailSubject='"Subscription publication Notice error!"';
	my $mailTo=$errorNoticeEmailTo;
	my $content=shift;
	$content='"'.$content.'"';
	my $cmd="cd $bin_dir && java -jar emailReport.jar $mailSubject $content $mailTo";
	
}

sub runPSQL{
	my $query=shift;
	
	$query=trim($query);
	my @cmd=`psql -d $default_database -c "$query" 2>$bin_dir/log/$$.log`;
	my @error=`cat $bin_dir/log/$$.log`;
	system("rm -rf $bin_dir/log/$$.log");
	my %result=(
		'stout' => \@cmd,
		'erout' => \@error
	);
	return %result;
}
return 1;
exit;