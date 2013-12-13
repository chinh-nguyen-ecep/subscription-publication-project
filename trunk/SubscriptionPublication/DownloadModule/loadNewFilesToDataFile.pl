@cmd=`cat newFiles.log`;
print @cmd;
foreach $temp(@cmd){
	system($temp);
}
system("rm -rf newFiles.log");