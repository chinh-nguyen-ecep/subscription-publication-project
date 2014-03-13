package ecepvn.org.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
/**
 * The class create connection to postgres database 9 on vls001
 * @author Chinh nguyen
 */
public class ConnectDB {
	/**
	 * Get connection to PostgreSQL database.
	 * 
	 * @param
	 * 
	 * @return Connection object
	 * @throws IOException 
	 * 
	 */
	public static synchronized Connection getConnection() throws IOException {
		Connection connection = null;
		Properties prop = new Properties();
		// the configuration file name
        String fileName = "config/emailAndJasperGenerateConfig.txt";            
        InputStream is = new FileInputStream(fileName);
        // load the properties file
        prop.load(is);
        String host=prop.getProperty("host");
        String port=prop.getProperty("port");
        String database=prop.getProperty("database");
        String user=prop.getProperty("dbuser");
        String pass=prop.getProperty("dbpassword");
        
		String strConnect = "jdbc:postgresql://"+host+":"+port+"/"+database;
//		String strConnect = "jdbc:postgresql://localhost:35432/maponics";
		try {
//			System.out.println(strConnect+"-"+user+"-"+pass);
			connection = DriverManager.getConnection(strConnect, user,
					pass);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		is.close();
		return connection;
	}
	public static synchronized Connection getConnection(String configFileName) throws IOException {
		Connection connection = null;
		Properties prop = new Properties();
		// the configuration file name
        String fileName = "config/"+configFileName;            
        InputStream is = new FileInputStream(fileName);
        // load the properties file
        prop.load(is);
        String host=prop.getProperty("host");
        String port=prop.getProperty("port");
        String database=prop.getProperty("database");
        String user=prop.getProperty("dbuser");
        String pass=prop.getProperty("dbpassword");
        
		String strConnect = "jdbc:postgresql://"+host+":"+port+"/"+database;
//		String strConnect = "jdbc:postgresql://localhost:35432/maponics";
		try {
//			System.out.println(strConnect+"-"+user+"-"+pass);
			connection = DriverManager.getConnection(strConnect, user,
					pass);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		is.close();
		return connection;
	}
	/**
	 * Get ResultSet of executing a query
	 * 
	 * @param String
	 *            query user's input query
	 * @return ResultSet object
	 * 
	 */
	public static ResultSet getResultSet(String query) {
		ResultSet rs = null;
		try {
			Connection conn = getConnection();
			Statement stat = conn.createStatement();
			rs = stat.executeQuery(query);
			conn.close();
		} catch (Exception e) {
		}
		;
		return rs;
	}

	/**
	 * Return a String array represents the data of the recently Result set.
	 * 
	 * @param String
	 *            query user's input query
	 * @return ArrayList object
	 * 
	 */
	public static ArrayList<String[]> runSelectQuery(String query)
			throws SQLException {
		ResultSet rs = ConnectDB.getResultSet(query);
		if (rs == null) {
			ArrayList<String[]> list = new ArrayList<String[]>();
			return list;
		}
		int countColum = rs.getMetaData().getColumnCount();
		String[] columName = new String[countColum];

		for (int i = 1; i <= countColum; i++) {
			String name = rs.getMetaData().getColumnName(i);
			columName[i - 1] = name;
		}

		ArrayList<String[]> list = new ArrayList<String[]>();
		list.add(columName);
		while (rs.next()) {
			String[] temp = new String[countColum];
			for (int i = 1; i <= countColum; i++) {
				temp[i - 1] = rs.getString(i);
			}
			list.add(temp);
		}
		return list;
	}

}