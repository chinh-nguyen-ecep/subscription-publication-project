import com.amazonaws.services.s3.AmazonS3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import utils.Encryptor;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

//	2014-08-28 
//	Download files from S3 at vere-dw/outgoing/agg/ais
// 	This script run on pentaho-aws Then move download files to dw12 and insert a notice into control.data_file on dw12
//	
public class S3DownloadAggregateFromAISToDw12 {
	private AmazonS3 s3;
	private static String cofigFileName="config/s3DownloadConfig.txt";
	private String folder="";
	private String destinationFolder="";
	private String bucketName="";
	private String dataFileConfigId="";
	private String importDir="";
	private int time_refresh=5;
	
	public S3DownloadAggregateFromAISToDw12() throws IOException{
		//Loading config infomation
		Properties prop = new Properties();
		// the configuration file name
	    String fileName = "config/s3DownloadConfig.txt";            
	    InputStream is;
		is = new FileInputStream(fileName);
		prop.load(is);
		this.bucketName = prop.getProperty("bucketName");
		this.folder = prop.getProperty("folder");
		this.destinationFolder=prop.getProperty("destinationFolder");
		this.dataFileConfigId=prop.getProperty("data_file_config_id");
		this.importDir=prop.getProperty("import_dir");
		this.time_refresh=Integer.parseInt(prop.getProperty("time_refresh"));
		is.close();
		while(true){
			try{
				s3 = new AmazonS3Client(new PropertiesCredentials(S3Download.class.getResourceAsStream("AwsCredentials.properties")));
				// Listing Objects
				ObjectListing currentList = s3.listObjects(bucketName, folder);
				ArrayList<S3ObjectSummary> listFiles = new ArrayList<S3ObjectSummary>();
				  do {
				   for (S3ObjectSummary objectSummary : currentList
				     .getObjectSummaries()) {
				    listFiles.add(objectSummary);
				   }
				   currentList = s3.listNextBatchOfObjects(currentList);
				  } while (currentList.isTruncated());
	
				  for (S3ObjectSummary objectSummary : currentList.getObjectSummaries()) {
				   listFiles.add(objectSummary);
				  }
				downloader(listFiles);
				System.out.println("#"+new Date().toString()+"\nRefresh next process start after "+this.time_refresh+"s");
				TimeUnit.SECONDS.sleep(this.time_refresh);
			}catch (Exception e) {
				// TODO: handle exception
			}
		}	
	}

	private void downloader(ArrayList<S3ObjectSummary> listObjects) {
		File myDestinationFolder=new File(this.destinationFolder);
		if(!myDestinationFolder.exists()){
			myDestinationFolder.mkdir();
		}
		System.out.println("Download file to: "+myDestinationFolder.getPath());
		for (S3ObjectSummary object : listObjects) {
			String key = object.getKey();
			long size = object.getSize();			
			File checkExist = new File(key);
			checkExist=new File(this.destinationFolder+"/"+checkExist.getName());			
			if (!checkExist.exists()) {
				// Get Object
				if(checkExist.isDirectory()){
					//checkExist.mkdirs();					
				}else{
					S3Object s3Object = s3.getObject(this.bucketName, key);
					System.out.println("Downloading file: " + key);
					System.out.println("File size: "+size);
					fileDownloader(s3Object.getObjectContent(), key,myDestinationFolder.getPath());	
				}
			} else {
//				System.out.println("File: " + checkExist.getName() + " existed! Abort download!");
			}
				
		}
	}

	private void fileDownloader(InputStream input, String fileName,String destinationFolderFullPath) {
		File fileInput = new File(fileName);
		String fileOutputName=fileInput.getName();
		String fileOutput=destinationFolderFullPath+"//"+fileOutputName;
		System.out.println("File output:"+fileOutput);		
		if(checkFileName(fileInput.getName())){
			// Write to file
			OutputStream out;
			try {
				out = new FileOutputStream(fileOutput);

				byte[] buf = new byte[1024];
				int len;

				while ((len = input.read(buf)) > 0) {
					out.write(buf, 0, len);
				}
				input.close();
				out.close();
				
				//get file_timestamp from file
				String file_timestamp="now()::timestamp without time zone";
				String[] array=fileOutputName.split("\\.");
				if(array[0].equals("daily")){
					file_timestamp="'"+array[3]+"'::timestamp without time zone";						
				}else if(array[0].equals("date_range")){
					file_timestamp="'"+array[4]+"'::timestamp without time zone";							
				}
				//Copy file to import dir
				boolean coped=copyFileSFTP(fileOutput,new File(this.importDir).getPath());	
				if(coped){
					String insertComand="INSERT INTO control.data_file(file_name,server_name,file_timestamp,data_file_config_id,file_status,dt_file_queued)" +
							" VALUES ('"+fileOutputName+"','s3',"+file_timestamp+","+this.dataFileConfigId+",'ER',now()::timestamp without time zone) RETURNING data_file_id";
					
					try{
						Connection conn=ConnectDB.getConnection();
						PreparedStatement st=conn.prepareStatement(insertComand);
						ResultSet rs=st.executeQuery();
						if(rs.next()){
							//this.max_data_file_id=rs.getInt(1);
						}
						st.close();
						conn.close();	
						clearFile(fileOutput);
					}catch (SQLException e) {
						// TODO: handle exception
						if(e.getMessage().indexOf("duplicate key value violates")>-1){
							System.err.println("ERROR: duplicate key value violates unique constraint \"unq_data_file_on_file_name\" "+fileOutputName);
							clearFile(fileOutput);
						}else{
							System.err.println(e.getMessage());
							File delFile=new File(fileOutput);
							delFile.deleteOnExit();
						}
					}
				}else{
					File delFile=new File(fileOutput);
					delFile.deleteOnExit();
				}
				

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				File delFile=new File(fileOutput);
				delFile.deleteOnExit();
				//fileDownloader(input, fileName,destinationFolderFullPath);
			}					
		}else{
				System.out.println("File name wrong format");
		}

	}
	
	private boolean checkFileName(String fileName){
		fileName=fileName.trim();
		String[] array=fileName.split("\\.");
		if(array.length<6){
			return false;			
		}else{
			String mode=array[0];
			System.out.println("File type:"+array[0]);
			if(mode.equals("daily") ||  mode.equals("date_range") || mode.equals("monthly") || mode.equals("weekly")){
				return true;
			}else{
				return false;				
			}
			
		}
		
	}
	
	private boolean copyFileSFTP(String file,String importDir) throws IOException{
		boolean result=false;
		//Load configure
		InputStream is = new FileInputStream(cofigFileName);
        Properties prop = new Properties();
        prop.load(is);
		String sftphost=prop.getProperty("sftphost");	
		String sftpport=prop.getProperty("sftpport");
		String sftpusername=prop.getProperty("sftpusername");
		String sftppass=Encryptor.decrypt(prop.getProperty("sftppass"));
		is.close();			
		
		String SFTPHOST = sftphost;
		int    SFTPPORT = Integer.parseInt(sftpport);
		String SFTPUSER = sftpusername;
		String SFTPPASS = sftppass;
		String privateKey = "/home/"+SFTPUSER+"/.ssh/id_rsa";
		String SFTPWORKINGDIR = importDir;
		Session     session     = null;
		Channel     channel     = null;
		ChannelSftp channelSftp = null;
		 
		try{
		            JSch jsch = new JSch();
		            if(SFTPPASS.equals("")){
		            	jsch.addIdentity(privateKey);
		            }
		            session = jsch.getSession(SFTPUSER,SFTPHOST,SFTPPORT);
		            session.setPassword(SFTPPASS);
		            java.util.Properties config = new java.util.Properties();
		            config.put("StrictHostKeyChecking", "no");
		            session.setConfig(config);
		            session.connect();
		            channel = session.openChannel("sftp");
		            channel.connect();
		            channelSftp = (ChannelSftp)channel;
		            channelSftp.cd(SFTPWORKINGDIR);
		            File f = new File(file);
		            channelSftp.put(new FileInputStream(f), f.getName());
		            result=true;
		            session.disconnect();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return result;
	}
	private void clearFile(String filePath){
		 File file = new File(filePath);
		 PrintWriter writer;
		try {
			writer = new PrintWriter(file);
			 writer.print("");
			 writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args) throws IOException {
		
		S3DownloadAggregateFromAISToDw12 s3Downloader = new S3DownloadAggregateFromAISToDw12();
		
	}

}
