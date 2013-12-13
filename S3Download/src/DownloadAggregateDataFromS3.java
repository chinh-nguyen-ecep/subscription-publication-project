import com.amazonaws.services.s3.AmazonS3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class DownloadAggregateDataFromS3 {
	private AmazonS3 s3;
	private String newFilesLogName="newFiles.log";
	public DownloadAggregateDataFromS3(String bucketName, String folder,String destinationFolder) throws IOException, InterruptedException {
		File logNewFile=new File(newFilesLogName);
		if(!logNewFile.exists()){
			logNewFile.createNewFile();
		}else{
			FileReader fr = new FileReader(logNewFile.getAbsoluteFile());
			BufferedReader br = new BufferedReader(fr);			
			String eachLine = br.readLine();
			if(eachLine!=null){
				System.out.println("New files need load:");
				while (eachLine != null) {
					System.out.println(eachLine);
					eachLine = br.readLine();
				}
				br.close();
				fr.close();
				
			}else{
				System.out.println("Have a loading process is runing! Out off...");
			}
			return;
		}
		
		// Import Credential Key
		try {
			s3 = new AmazonS3Client(new PropertiesCredentials(S3Download.class.getResourceAsStream("AwsCredentials.properties")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
		  
		downloader(listFiles, bucketName,destinationFolder);

		// Download Object

		System.out.println("Done.");
		runShellComand("perl loadNewFilesToDataFile.pl");
	}

	private void downloader(ArrayList<S3ObjectSummary> listObjects, String bucketName, String destinationFolder) {
		File myDestinationFolder=new File(destinationFolder);
		if(!myDestinationFolder.exists()){
			myDestinationFolder.mkdir();
		}
		System.out.println("Download file to: "+myDestinationFolder.getPath());
		for (S3ObjectSummary object : listObjects) {
			String key = object.getKey();
			long size = object.getSize();			
			File checkExist = new File(key);
			checkExist=new File(destinationFolder+"/"+checkExist.getName());			
			if (!checkExist.exists()) {
				// Get Object
				if(checkExist.isDirectory()){
					//checkExist.mkdirs();					
				}else{
					S3Object s3Object = s3.getObject(bucketName, key);
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
				// the configuration file name
		        String cofigFileName = "config/s3DownloadConfig.txt";            
		        InputStream is = new FileInputStream(cofigFileName);
		        Properties prop = new Properties();
		        prop.load(is);
				String database=prop.getProperty("database");	
				String dataFileConfigId=prop.getProperty("data_file_config_id");
				String importDir=prop.getProperty("import_dir");
				String userName=prop.getProperty("userName");
				is.close();			
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
				
				String insertComand="psql -U "+userName+" -d "+database+" -c \"INSERT INTO control.data_file(file_name,server_name,file_timestamp,data_file_config_id,file_status,dt_file_queued)" +
						" VALUES ('"+fileOutputName+"','s3',"+file_timestamp+","+dataFileConfigId+",'ER',now()::timestamp without time zone)\" ";
				writeNewFileToLog(insertComand);
				

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//fileDownloader(input, fileName,destinationFolderFullPath);
			}					
		}else{
				System.out.println("File name wrong format");
		}

	}
	
	private boolean checkFileName(String fileName){
		boolean result=true;
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
	
	private boolean copyFile(String file,String importDir) throws IOException{
		boolean result=false;
	   	InputStream inStream = null;
		OutputStream outStream = null;
	 
	    	
	 
	    	    File afile =new File(file);
	    	    File bfile =new File(importDir+"/"+afile.getName());
	    	    if(bfile.exists()){
	    	    	bfile.delete();	    	    	
	    	    }
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
	 
	    	    System.out.println("File is copied successful!");
	    	    result=true;
		
		return result;		
	}
	
	private void runShellComand(String comand) throws IOException, InterruptedException{
		System.out.println("Run comand:"+comand);
		java.lang.Runtime rt = java.lang.Runtime.getRuntime();
		java.lang.Process p = rt.exec(comand.trim());
		p.waitFor();
		BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";

		while ((line = b.readLine()) != null) {
		  System.out.println(line);
		}
		b.close();
	}
	
	private void writeNewFileToLog(String insertComand) throws IOException{
		File file = new File(newFilesLogName);
		FileWriter writer = new FileWriter(file, true);
		writer.write(insertComand+"\n");
	    writer.flush();
	    writer.close();
	}
	public static void main(String[] args) throws IOException, InterruptedException {
		Properties prop = new Properties();
		// the configuration file name
        String fileName = "config/s3DownloadConfig.txt";            
        InputStream is = new FileInputStream(fileName);
        prop.load(is);
		String bucket = prop.getProperty("bucketName");
		String folder = prop.getProperty("folder");
		String destinationFolder=prop.getProperty("destinationFolder");
		is.close();
		if(destinationFolder==null){
			destinationFolder="";				
		}
		DownloadAggregateDataFromS3 s3Downloader = new DownloadAggregateDataFromS3(bucket, folder,destinationFolder);
		
//
//		String bucket="ecep";
//		String folder="subscription_publication_app/data/outgoing/daily";
//		String destinationFolder="";
//		S3Download s3Downloader = new S3Download(bucket, folder,destinationFolder);
	}

}
