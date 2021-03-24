package e3mstore_test;
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

public class S_ItemCondition {

	public static void main(String[] args) {

		Connection conn = null;
		try {
			
			FileReader reader=new FileReader("Y://maxmobile//TEST//Storekeeper//Config.properties");  
			
			Properties prop=new Properties();  
			prop.load(reader);  
		
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
					prop.getProperty("bulkdata.db.password"));

			List<String[]> data = new ArrayList<String[]>();
		    String exQuery="SELECT CONDITIONCODE,CONDRATE,DESCRIPTION,ITEMCONDITIONID,ITEMNUM,ITEMSETID FROM MAXIMO.ITEMCONDITION";
			PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\ITEMCONDITION.csv"); 
			
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				
				data.add(new String[]{rsData.getString("CONDITIONCODE")
						+ "",""+rsData.getString("CONDRATE")
						+"",""+rsData.getString("DESCRIPTION") 
						+ "",""+rsData.getString("ITEMCONDITIONID")
						+ "",""+rsData.getString("ITEMNUM")
						+ "",""+rsData.getString("ITEMSETID")
						});
			
		
			}
	        String[] header = "CONDITIONCODE,CONDRATE,DESCRIPTION,ITEMCONDITIONID,ITEMNUM,ITEMSETID".split(",");	        
	        
	        writer.writeNext(header);
	        writer.writeAll(data);
	        System.out.println("CSV Writing complete");
	        writer.close(); 
		    
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


	