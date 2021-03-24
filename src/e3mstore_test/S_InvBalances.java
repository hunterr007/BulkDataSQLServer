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

public class S_InvBalances {

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
		    String exQuery="SELECT INV.INVENTORYID,I.INVBALANCESID,I.ITEMNUM,I.ITEMSETID,I.PHYSCNTDATE,I.RECONCILED,I.SITEID,I.CURBAL,I.ORGID,\r\n" + 
					"I.BINNUM,I.CONDITIONCODE,I.LOTNUM,I.LOCATION,INV.CCF,IT.DESCRIPTION,I.PHYSCNT,INV.ABCTYPE,INV.STATUS\r\n" + 
					"FROM MAXIMO.INVBALANCES I\r\n" + 
					"INNER JOIN MAXIMO.INVENTORY INV\r\n" + 
					"ON I.ITEMNUM = INV.ITEMNUM AND I.LOCATION = INV.LOCATION AND I.ITEMSETID = INV.ITEMSETID AND I.SITEID = INV.SITEID AND\r\n" + 
					"INV.STATUS IN  ('ACTIVE','PLANNING','PENDOBS')\r\n" + 
					"LEFT JOIN MAXIMO.ITEM IT\r\n" + 
					"ON IT.ITEMNUM = I.ITEMNUM AND IT.ITEMSETID = I.ITEMSETID";
		    
			PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\INVBALANCES.csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        

			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				

				data.add(new String[]
						{rsData.getString("INVENTORYID")
						+ "",""+rsData.getString("INVBALANCESID")
						+ "",""+rsData.getString("ITEMNUM")
						+ "",""+rsData.getString("ITEMSETID")
						+ "",""+rsData.getString("PHYSCNTDATE")
						+ "",""+rsData.getString("RECONCILED")
						+ "",""+rsData.getString("SITEID") 
						+ "",""+rsData.getString("CURBAL") 
						+ "",""+rsData.getString("ORGID")
						+ "",""+rsData.getString("BINNUM")
						+ "",""+rsData.getString("CONDITIONCODE")
						+ "",""+rsData.getString("LOTNUM")
						+ "",""+rsData.getString("LOCATION")
						+ "",""+rsData.getString("CCF")
						+ "",""+rsData.getString("DESCRIPTION")
						+ "",""+rsData.getString("PHYSCNT")
						+ "",""+rsData.getString("ABCTYPE")
						+ "",""+rsData.getString("STATUS")
						});
			
			}
	        String[] header = "INVENTORYID,INVBALANCESID,ITEMNUM,ITEMSETID,PHYSCNTDATE,RECONCILED,SITEID,CURBAL,ORGID,BINNUM,CONDITIONCODE,LOTNUM,LOCATION,CCF,DESCRIPTION,PHYSCNT,ABCTYPE,STATUS".split(",");	        
	        
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
