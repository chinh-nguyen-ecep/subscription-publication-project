package ecepvn.org.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

	/**
	 * @param args
	 */
	public static void note(String text){		
	    Date dNow = new Date( );
	    SimpleDateFormat ft = 
	    new SimpleDateFormat ("yyyy.MM.dd hh:mm:ss a");

	    System.out.println("#" + ft.format(dNow)+" - "+text);
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		note("Hello");
	}

}
