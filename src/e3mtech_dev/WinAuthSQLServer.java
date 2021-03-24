package e3mtech_dev;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVWriter;

public class WinAuthSQLServer {

	public static void main(String[] args) {

		Connection conn = null;
		try {
			
			
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String connectionString = "jdbc:sqlserver://;serverName=localhost;databaseName=master;integratedSecurity=true;";
	        String uName="RFL207\\prashant";
	        String pWord="bareilly";
			conn = DriverManager.getConnection(connectionString,uName,pWord);
			
			
		    String exQuery="select * from dbo.MSreplication_options"; 
           
		    PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				System.out.println(rsData.getString("optname"));		
			}
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
       
		finally
		{
	        try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
        
	}

	

}
