
sub getConnection{
	my $host=$defaul_host;
	my @values = split(':', $host);
	my $host_name='';
	my $port=$defaul_port;
	my $database=$default_database;
	my $userName=$defaul_userName;
	my $pass=$defaul_pass;	
	my $conn='';	
	
	# Process input param
	if(@values==1){
		$host_name=$host;
	}elsif(@values==2){
		$host_name=$values[0];
		$database=$values[1];
	}elsif(@values==3){
		$host_name=$values[0];
		$database=$values[1];
		$port=$values[2];
	}
	         
	

	$conn=DBIx::AutoReconnect-> connect(
	   "dbi:PgPP:dbname=$database;host=$host_name;$port",
	   $userName,
	   $pass,
	   {
			PrintError => 0,
			ReconnectTimeout => 60,
			ReconnectFailure => sub { warn "oops $host_name $port $database!" },
			ReconnectMaxTries => 100
	   },
	);
	return $conn;
}

sub sqlTest{
	my $dbh=shift;
	my $query="select relname from pg_stat_user_tables ORDER BY relname LIMIT 10 ;";
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$relname);		
		# LOOP THROUGH RESULTS
		while($query_handle->fetch()) {
		   print "Table: $relname\n";
		} 
}
sub sqlDisconnect{
	my $dbh=shift;
	my $rv = $dbh->disconnect;
}
return 1;
exit;