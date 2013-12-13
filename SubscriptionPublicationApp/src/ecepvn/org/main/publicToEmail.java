package ecepvn.org.main;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ecepvn.org.utils.ConnectDB;
import ecepvn.org.utils.Utils;

public class publicToEmail {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 */
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		
		int publication_process_id=Integer.parseInt(args[0]);
		String processString=ManagementFactory.getRuntimeMXBean().getName();
		int processID=Integer.parseInt(processString.split("@")[0]);
		
		Utils.note("Start with processID: "+processID);
		try {
		//Update status processs	
		dbConnection=ConnectDB.getConnection();
		
		String query="UPDATE control.spctl_subscription_publication_process " +
				"SET process_status=? ,dt_lastchange=now() " +
				"WHERE publication_process_id=?";
		
		preparedStatement =dbConnection.prepareStatement(query);
		preparedStatement.setString(1, "PST");
		preparedStatement.setInt(2, publication_process_id);
		preparedStatement.executeUpdate();		
		
		//Load config data
		String subscription_attribute="";
		String process_actribute="";
		String export_dir="";
		
		query="SELECT " +
				"b.process_actribute" +
				",c.subscription_attribute" +
				",g.export_dir" +
				",h.customer_host_name" +
				",h.customer_destination_folder" +
				",a.export_file_name" +
				",a.file_size" +
				",c.frequence" +
				",c.zip_before_transfer" +
				",h.transfer_script_name" +
				",h.folder_content_transfer_script" +
				",a.customer_article_key" +
				",c.subscription_name" +
			" FROM " +
				"control.spctl_subscription_publication_process_concurrent_trans a" +
				",control.spctl_subscription_publication_process b" +
				",control.spctl_pub_customer_subscription c" +
				",control.spctl_pub_customer_article d" +
				",control.spctl_data_file_config e" +
				",control.spctl_data_source_tables f" +
				",control.spctl_export_module g" +
				",control.spctl_pub_customer h" +
			" WHERE " +
				"a.publication_process_id=?" +
				" AND a.publication_process_id=b.publication_process_id" +
				" AND b.subscription_key=c.subscription_key" +
				" AND a.customer_article_key=d.customer_article_key" +
				" AND d.df_config_id=e.df_config_id" +
				" AND e.data_source_table_id=f.data_source_table_id" +
				" AND e.export_module_id=g.export_module_id	" +
				" AND c.customer_key=h.customer_key";
		preparedStatement =dbConnection.prepareStatement(query);
		preparedStatement.setInt(1, publication_process_id);
		ResultSet rs = preparedStatement.executeQuery();
		while(rs.next()){
			
			
		}
		} catch (SQLException e) {
			 
			System.out.println(e.getMessage());
 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally { 
			if (preparedStatement != null) {
				preparedStatement.close();
			}
 
			if (dbConnection != null) {
				dbConnection.close();
			} 
		}
	}

}
