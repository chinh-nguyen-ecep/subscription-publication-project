package ecepvn.org.app;

import java.io.IOException;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import ecepvn.org.utils.SendFileEmail;

public class emailReport {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException {
		// TODO Auto-generated method stub
		if(args.length<=1){
			if(args[0].equals("--help")){
				System.out.println("input: [mail subject] [mail message] [email address] [attachment file]...[attachment file]");
				System.out.println("Example: java -jar emailReport.jar \"Testing\" \"Hi Guess!\" abc@mail.com,dbc@mail.com d:\\acv.pdf d:\\acv.xls .... ");
			}
		}else if(args.length>=3){
			String subject=args[0];
			String text=args[1];
			String mailAdress=args[2];
			
			System.out.println("Email Subject: "+subject);
			System.out.println("Email message: "+text);
			System.out.println("Email Address: "+mailAdress);
			System.out.println("Email Attachments: ");
			
			String[] temp=new String[args.length-3];
			for(int i=0;i<args.length-3;i++){
				temp[i]=args[i+3];	
				System.out.println(temp[i]);
			}
			SendFileEmail.send(subject,text,mailAdress, temp);
			
		}
	}

}
