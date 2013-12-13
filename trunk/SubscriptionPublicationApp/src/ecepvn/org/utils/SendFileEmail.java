package ecepvn.org.utils;

// File Name SendFileEmail.java

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;

public class SendFileEmail
{

	
	public static void send(String subject,String messageText,String email,String[] fullFilePath) throws IOException, AddressException, MessagingException{
		// get the config 
		final Properties props  = new Properties();
        String fileName = "config/emailAndJasperGenerateConfig.txt";            
        InputStream is = new FileInputStream(fileName);
        props.load(is);
        is.close();
		Session session = Session.getDefaultInstance(props,
		new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(props.getProperty("mail.smtp.user"),props.getProperty("mail.smtp.pass"));
			}
		});
		
			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(props.getProperty("mail.smtp.user")));
			message.setRecipients(Message.RecipientType.TO,InternetAddress.parse(email));
			message.setSubject(subject);

			Multipart multipart = new MimeMultipart();
			// create the message part 
		    MimeBodyPart messageBodyPart = new MimeBodyPart();
		    //message body
		    messageBodyPart.setText(messageText);
		    messageBodyPart.setContent(messageText, "text/html");
		    multipart.addBodyPart(messageBodyPart);
		    for(int i=0;i<fullFilePath.length;i++){	
			    //attachment
			    messageBodyPart = new MimeBodyPart();
			    DataSource source = new FileDataSource(fullFilePath[i]);
			    messageBodyPart.setDataHandler(new DataHandler(source));
			    messageBodyPart.setFileName(source.getName());
			    multipart.addBodyPart(messageBodyPart);
		    }
		    message.setContent(multipart);		
		    //send message to reciever
		    Transport.send(message);
			System.out.println("Send report to email - Done");
		
	}
	
   public static void main(String [] args) throws IOException, AddressException, MessagingException
   {
	   String[] files=new String[3] ;
	   files[0]="D:\\testReport\\Weekly report 2013-W23.pdf";
	   files[1]="D:\\testReport\\Weekly report 2013-W23.xls";
	   files[2]="D:\\testReport\\week_year_report.2013-W23.pdf";
      SendFileEmail.send("Test mail","<b>Hi chinh</b> <p />This the testing mail ","chinh.nguyen@ecepvn.org", files);
 
   }
}