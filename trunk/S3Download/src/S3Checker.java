import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class S3Checker {

	/**
	 * @param args
	 */
	private AmazonS3 s3;
	private String newFilesLogName="newFiles.log";
	private int process_config_id=0;
	private int runing_process_id=0;
	private String folder="";
	private String destinationFolder="";
	private String bucketName="";
	private String database="";
	private String dataFileConfigId="";
	private String importDir="";
	private String userName="";
	private int max_data_file_id=0;
	private int time_refresh=5;
	public S3Checker()  {
		//Loading config infomation
		Properties prop = new Properties();
		// the configuration file name
        String fileName = "config/s3DownloadConfig.txt";            
        InputStream is;
		try {
			is = new FileInputStream(fileName);
			prop.load(is);
			this.bucketName = prop.getProperty("bucketName");
			this.folder = prop.getProperty("folder");
			this.destinationFolder=prop.getProperty("destinationFolder");
			this.process_config_id=Integer.parseInt(prop.getProperty("process_config_id"));
			this.database=prop.getProperty("database");
			this.dataFileConfigId=prop.getProperty("data_file_config_id");
			this.importDir=prop.getProperty("import_dir");
			this.userName=prop.getProperty("userName");
			this.time_refresh=Integer.parseInt(prop.getProperty("time_refresh"));
			is.close();
			//Insert a process into control.process
			this.runing_process_id=registerProcess();
			if(this.runing_process_id<0){
				System.out.println("===========================\nRegister to process failed!");
				return;
			}
			System.out.println("Register a new process: "+this.runing_process_id);
			while(true){		
				updateProcessStatus();
				s3 = new AmazonS3Client(new PropertiesCredentials(S3Download.class.getResourceAsStream("AwsCredentials.properties")));
				// Listing Objects
				ObjectListing currentList = s3.listObjects(this.bucketName, this.folder);
				ArrayList<S3ObjectSummary> listFiles = new ArrayList<S3ObjectSummary>();

				do {
					  for (S3ObjectSummary objectSummary : currentList.getObjectSummaries()) {
						  listFiles.add(objectSummary);
					  }
					  currentList = s3.listNextBatchOfObjects(currentList);
				} while (currentList.isTruncated());
				for (S3ObjectSummary objectSummary : currentList.getObjectSummaries()) {
				   listFiles.add(objectSummary);
				}
				System.out.println("List file size: "+listFiles.size());
				downloader(listFiles);
				updateProcessSL();
				System.out.println("#"+new Date().toString()+"\nRefresh next process start after "+this.time_refresh+"s");
				TimeUnit.SECONDS.sleep(this.time_refresh);
			}
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				//insert to data file
				//get file_timestamp from file
				String file_timestamp="now()::timestamp without time zone";
				String[] array=fileOutputName.split("\\.");
				if(array[0].equals("daily")){
					file_timestamp="'"+array[3]+"'::timestamp without time zone";						
				}else if(array[0].equals("date_range")){
					file_timestamp="'"+array[4]+"'::timestamp without time zone";							
				}
				//Copy file to import dir
				boolean coped=copyFile(fileOutput,new File(importDir).getPath());	
				if(coped){
					//insert to data file
					String insertComand="psql -U "+userName+" -d "+database+" -c \"INSERT INTO control.data_file(file_name,server_name,file_timestamp,data_file_config_id,file_status,dt_file_queued)" +
							" VALUES ('"+fileOutputName+"','s3',"+file_timestamp+","+dataFileConfigId+",'ER',now()::timestamp without time zone)\" ";
					
					insertComand="INSERT INTO control.data_file(file_name,server_name,file_timestamp,data_file_config_id,file_status,dt_file_queued)" +
							" VALUES ('"+fileOutputName+"','s3',"+file_timestamp+","+dataFileConfigId+",'ER',now()::timestamp without time zone) RETURNING data_file_id";
					
					Connection conn=ConnectDB.getConnection();
					PreparedStatement st=conn.prepareStatement(insertComand);
					ResultSet rs=st.executeQuery();
					if(rs.next()){
						this.max_data_file_id=rs.getInt(1);
					}
					st.close();
					conn.close();	
					
				}else{
					File delFile=new File(fileOutput);
					delFile.deleteOnExit();
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fileDownloader(input, fileName,destinationFolderFullPath);
			}					
		}else{
				System.out.println("Wrong format file name!");
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
	
	private boolean copyFile(String file,String importDir){
		boolean result=false;
	   	InputStream inStream = null;
		OutputStream outStream = null;
	 
	    	    File afile =new File(file);
	    	    File bfile =new File(importDir+"/"+afile.getName());
	    	    if(bfile.exists()){
	    	    	bfile.delete();	    	    	
	    	    }
	    	    try {
					inStream = new FileInputStream(afile);
					outStream = new FileOutputStream(bfile);
		    	    byte[] buffer = new byte[1024];
		    	    int length;
		    	    //copy the file content in bytes 
		    	    while ((length = inStream.read(buffer)) > 0){
		    	    	outStream.write(buffer, 0, length);
		    	    }
		    	    inStream.close();
		    	    outStream.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					//copyFile(file,importDir);
					return false;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					//copyFile(file,importDir);
					return false;
				}
	    	    System.out.println("File is copied successful!");
	    	    result=true;
		
		return result;		
	}
	
	private int registerProcess() throws IOException, SQLException{
		int result=0;
		String query="INSERT INTO control.process(process_config_id,process_status,min_partition_key,max_partition_key,dt_process_queued,dt_lastchange) VALUES(?,?,1,1,now(),now()) RETURNING process_id";
			Connection conn=ConnectDB.getConnection();
			PreparedStatement st=conn.prepareStatement(query);
			st.setInt(1, process_config_id);
			st.setString(2, "PS");
			ResultSet rs=st.executeQuery();
			if(rs.next()){
				result=rs.getInt(1);
			}			
			conn.close();
		return result;
		
	}
	private void updateProcessStatus() throws IOException, SQLException{
		String query="UPDATE control.process SET process_status='PS',dt_lastchange=now(),dt_process_completed=now(),max_data_file_id=? WHERE process_id=?";
			Connection conn=ConnectDB.getConnection();
			PreparedStatement st=conn.prepareStatement(query);
			st.setInt(1, this.max_data_file_id);
			st.setInt(2, this.runing_process_id);
			st.executeUpdate();
			conn.close();
	}
	private void updateProcessSL() throws IOException, SQLException{
		String query="UPDATE control.process SET process_status='SL',dt_lastchange=now(),dt_process_completed=now(),max_data_file_id=? WHERE process_id=?";
			Connection conn=ConnectDB.getConnection();
			PreparedStatement st=conn.prepareStatement(query);
			st.setInt(1, this.max_data_file_id);
			st.setInt(2, this.runing_process_id);
			st.executeUpdate();
			conn.close();
	}
	private void updateProcessER() throws IOException, SQLException{
		String query="UPDATE control.process SET process_status='ER',dt_lastchange=now(),dt_process_completed=now(),max_data_file_id=? WHERE process_id=?";
			Connection conn=ConnectDB.getConnection();
			PreparedStatement st=conn.prepareStatement(query);
			st.setInt(1, this.max_data_file_id);
			st.setInt(2, this.runing_process_id);
			st.executeUpdate();
			conn.close();
	}
	public static void main(String[] args) {
		S3Checker s3Downloader = new S3Checker();
	}

}
