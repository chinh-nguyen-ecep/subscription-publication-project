// ## Author: LyTC


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Download {
	private AmazonS3 s3;

	public S3Download(String bucketName, String folder,String destinationFolder) {
		// Import Credential Key
		try {
			s3 = new AmazonS3Client(new PropertiesCredentials(S3Download.class.getResourceAsStream("AwsCredentials.properties")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Listing Objects
		ObjectListing listObjects = s3.listObjects(bucketName, folder);

		downloader(listObjects, bucketName,destinationFolder);

		// Download Object

		System.out.println("Done.");

	}

	private void downloader(ObjectListing listObjects, String bucketName, String destinationFolder) {
		for (S3ObjectSummary object : listObjects.getObjectSummaries()) {
			String key = object.getKey();
			long size = object.getSize();			
			File checkExist = new File(key);			
			if (!checkExist.exists()) {
				// Get Object
				if(checkExist.isDirectory()){
					//checkExist.mkdirs();					
				}else{
					S3Object s3Object = s3.getObject(bucketName, key);
					System.out.println("Downloading file: " + key + " - Size: "
							+ size);
					fileDownloader(s3Object.getObjectContent(), key,destinationFolder, listObjects,
							bucketName);

					System.out.println("Downloaded file: " + key + " - Size: "
							+ size);					
					
				}

			} else
				System.out
						.println("File: " + key + " existed! Abort download!");
		}
	}

	private void fileDownloader(InputStream input, String fileName,String destinationFolder,
			ObjectListing listObjects, String bucketName) {
		File file = new File(fileName);
		File myDestinationFolder=new File(destinationFolder);
		String fileOutput=file.getName();
		// Make Directory
		if(!destinationFolder.equals("")){			
			if(!myDestinationFolder.exists()){
				myDestinationFolder.mkdir();
			}else{
				fileOutput=myDestinationFolder.getPath()+"//"+fileOutput;				
			}				
		}
		
		System.out.println("File output:"+fileOutput);
		File checkFileOutput=new File(fileOutput);
		if(checkFileOutput.exists()){
			System.out.println("* File exits!");			
		}else{
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
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				downloader(listObjects, bucketName,destinationFolder);
			}					
		}

	}

	public static void main(String[] args) {
		if (args.length < 2)
			System.out.println("Usage: S3Downloader [bucket] [folder] [destinationFolder]");
		else {
			String bucket = args[0];
			String folder = args[1];
			String destinationFolder=args[2];
			if(args[2]==null){
				destinationFolder="";				
			}
			S3Download s3Downloader = new S3Download(bucket, folder,destinationFolder);
		}
//
//		String bucket="ecep";
//		String folder="subscription_publication_app/data/outgoing/daily";
//		String destinationFolder="";
//		S3Download s3Downloader = new S3Download(bucket, folder,destinationFolder);
	}
}
