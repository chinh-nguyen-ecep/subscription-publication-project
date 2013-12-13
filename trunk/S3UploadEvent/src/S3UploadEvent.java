import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3UploadEvent {
	private TransferManager s3;

	public S3UploadEvent(String bucketName, String folder) throws IOException,
			InterruptedException {
		// Import Credential Key
		s3 = new TransferManager(new PropertiesCredentials(S3UploadEvent.class.getResourceAsStream("AwsCredentials.properties")));

		ArrayList<File> fileList = new ArrayList<File>();
		File inputFolder = new File(folder);

		if (inputFolder.isDirectory())
			fileList.addAll(Arrays.asList(new File(folder).listFiles()));
		else
			fileList.add(inputFolder);

		long i = fileList.size();
		while (i > 0) {
			File file = fileList.get(0);

			if (file.isFile()) {
				String key = file.getParent() + "/" + file.getName();
				if (key.indexOf("/") == 0)
					key = key.substring(1);

				System.out.println("File stored on: s3://" + bucketName + "/"
						+ key);

				System.out.println("Uploading file: " + file.getName()
						+ " - Size: " + file.length());

				Upload upload = s3.upload(new PutObjectRequest(bucketName, key,
						file));
				try {
					// Or you can block and wait for the upload to finish
					upload.waitForCompletion();
				} catch (AmazonClientException amazonClientException) {
					System.out.println("Unable to upload file, upload was aborted.");
					amazonClientException.printStackTrace();
				}
				System.out.println("Upload completed: " + file.getName());
				System.out.println();
			}

			else {
				System.out
						.println("List files on directory: " + file.getName());
				fileList.addAll(Arrays.asList(file.listFiles()));
			}
			fileList.remove(0);
			i = fileList.size();
		}

		System.out.println("Done.");
		System.exit(1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (args.length < 2)
			System.out.println("Usage: S3Uploader [bucket] [folder OR file]");
		else {
			String bucket = args[0];
			String folder = args[1];
			S3UploadEvent S3Uploader = new S3UploadEvent(bucket, folder);
		}
	}
}
