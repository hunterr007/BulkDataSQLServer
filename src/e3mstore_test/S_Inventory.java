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

public class S_Inventory {

	public static void main(String[] args) {
		Connection conn = null;
		try {
			
			FileReader reader=new FileReader("Y://maxmobile//TEST//Storekeeper//Config.properties");  
			
			Properties prop=new Properties();  
			prop.load(reader);  
			int rCount=0;
			String codeFileName = "INVENTORY";
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
					prop.getProperty("bulkdata.db.password"));

			List<String[]> data = new ArrayList<String[]>();
			 String exQuery= "SELECT I.INVENTORYID,I.ITEMNUM,IT.DESCRIPTION,IT.LOTTYPE,IT.ROTATING,IT.CONDITIONENABLED,I.ITEMSETID,I.ABCTYPE,I.BINNUM,I.CCF,I.SITEID,I.CONTROLACC,\r\n" + 
					 "I.ORDERUNIT,I.ORGID,I.SHRINKAGEACC,I.SITEID,I.STATUS,I.STORELOC,\r\n" + 
				 		"I.LOCATION,I.STATUS,I.ISSUEUNIT,I.E3M_CURBAL,I.E3M_AVBAL \r\n" + 
				 		"FROM MAXIMO.INVENTORY I\r\n" + 
				 		"INNER JOIN MAXIMO.ITEM IT \r\n" + 
				 		"ON IT.itemsetid=I.itemsetid and  I.itemnum=IT.itemnum and IT.itemtype in \r\n" + 
				 		"(select value from MAXIMO.synonymdomain where domainid = 'ITEMTYPE'  and maxvalue = 'ITEM') \r\n" + 
				 		"WHERE I.STATUS IN  ('ACTIVE','PLANNING','PENDOBS')";
					 
					 
					 
			PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\INVENTORY.csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				
				
				data.add(new String[]{rsData.getString("INVENTORYID")
						+"",""+rsData.getString("ITEMNUM")
						+"",""+rsData.getString("DESCRIPTION")
						+"",""+rsData.getString("LOTTYPE")
						+"",""+rsData.getString("ROTATING")
						+"",""+rsData.getString("CONDITIONENABLED")
						+ "",""+rsData.getString("ITEMSETID")
						+ "",""+rsData.getString("ABCTYPE")
						+ "",""+rsData.getString("BINNUM")
						+ "",""+rsData.getString("CCF")
						+ "",""+rsData.getString("SITEID") 
						+ "",""+rsData.getString("CONTROLACC")
						+ "",""+rsData.getString("ORDERUNIT")
						+ "",""+rsData.getString("ORGID")
						+ "",""+rsData.getString("SHRINKAGEACC")
						+ "",""+rsData.getString("STORELOC")
						+ "",""+rsData.getString("LOCATION")
						+ "",""+rsData.getString("STATUS")
						+ "",""+rsData.getString("ISSUEUNIT")
						+ "",""+rsData.getString("E3M_CURBAL")
						+ "",""+rsData.getString("E3M_AVBAL")
						//+ "",""+rsData.getString("ASSETUID")
						});
				rCount=rCount+1;
		
			}
	        String[] header = "INVENTORYID,ITEMNUM,DESCRIPTION,LOTTYPE,ROTATING,CONDITIONENABLED,ITEMSETID,ABCTYPE,BINNUM,CCF,SITEID,CONTROLACC,ORDERUNIT,ORGID,SHRINKAGEACC,STORELOC,LOCATION,STATUS,ISSUEUNIT,E3M_CURBAL,E3M_AVBAL".split(",");	        
	        
	        writer.writeNext(header);
	        writer.writeAll(data);
	        System.out.println("CSV Writing complete "+ rCount);
	        writer.close(); 
	        
	        List<String[]> count = new ArrayList<String[]>();
	        File countFile = new File(prop.getProperty("bulkdata.file.destination")+"\\RecordCount.csv"); 
			FileWriter outputCountfile = new FileWriter(countFile,true); 	  
	        CSVWriter countWriter = new CSVWriter(outputCountfile); 
	        count.add(new String[]{codeFileName+ "",""+rCount});
	        countWriter.writeAll(count);
	        countWriter.close();
		    
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

