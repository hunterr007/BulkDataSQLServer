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

public class S_Asset {
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
			String exQuery="SELECT ASSETUID,ASSETID,ASSETNUM,LOCATION,DESCRIPTION,BINNUM,MOVED,RETURNEDTOVENDOR,STATUS,SITEID,ITEMNUM,SERIALNUM,E3MPRIVATEID FROM MAXIMO.ASSET\r\n" + 
		    		"WHERE ITEMNUM IS NOT NULL AND LOCATION IN\r\n" + 
		    		"(SELECT LOCATION TYPE FROM MAXIMO.LOCATIONS WHERE TYPE='STOREROOM')";
           

		    PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\ASSET.csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				
				data.add(new String[]{rsData.getString("ASSETUID")
						+ "",""+rsData.getString("ASSETID") 
						+ "",""+rsData.getString("ASSETNUM") + 
						"",""+rsData.getString("LOCATION") + "",""+
						rsData.getString("DESCRIPTION") + "",""+
						rsData.getString("BINNUM") + "",""+
						rsData.getString("MOVED") + "",""+
						rsData.getString("RETURNEDTOVENDOR") + "",""+
						rsData.getString("STATUS") + "",""+
						rsData.getString("SITEID")+ "",""+
						rsData.getString("ITEMNUM")+ "",""+
						rsData.getString("SERIALNUM")
						});
		
		
			}
	        String[] header = "ASSETUID,ASSETID,ASSETNUM,LOCATION,DESCRIPTION,BINNUM,MOVED,RETURNEDTOVENDOR,STATUS,SITEID,ITEMNUM,SERIALNUM".split(",");	        
	         
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
