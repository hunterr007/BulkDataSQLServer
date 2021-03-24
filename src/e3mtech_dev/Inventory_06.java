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

public class Inventory_06 {

	public static void main(String[] args) {
		Connection conn = null;
		try {
			
			/************** ENSURE ESCLATIONS HAVE RUN*************/
			
			
			FileReader reader=new FileReader("C://Users//prashant//Documents//EclipseWorkspace//BulkDataSQLServer//Config.properties");  
			
			Properties prop=new Properties();  
			prop.load(reader);  
			int rCount=0;
			String codeFileName="INVENTORY";
			
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
					prop.getProperty("bulkdata.db.password"));

			List<String[]> data = new ArrayList<String[]>();
			 String exQuery="SELECT I.INVENTORYID,I.ITEMNUM,I.ITEMSETID,I.SITEID,I.LOCATION,I.STATUS,I.ISSUEUNIT,I.E3M_CURBAL,I.E3M_AVBAL,A.ASSETUID\r\n" + 
			    		"FROM INVENTORY I\r\n" + 
			    		"LEFT JOIN ASSET A\r\n" + 
			    		"ON (A.ITEMNUM = I.ITEMNUM AND A.LOCATION = I.LOCATION AND \r\n" + 
			    		//"A.STATUS NOT IN ('INACTIVE','DECOMMISSIONED') AND\r\n" + 
						"A.STATUS NOT IN (select value from synonymdomain where domainid = 'LOCASSETSTATUS' and maxvalue in ('DECOMMISSIONED')) AND\r\n" + 
			    		"A.SITEID=I.SITEID AND\r\n" + 
			    		"A.ASSETNUM NOT IN\r\n" + 
			    		"(SELECT\r\n" + 
			    		"ROTASSETNUM FROM INVUSELINESPLIT\r\n" + 
			    		"WHERE\r\n" + 
			    		"ITEMNUM=A.ITEMNUM AND\r\n" + 
			    		"ITEMSETID=A.ITEMSETID AND\r\n" + 
			    		"FROMSTORELOC=A.LOCATION AND\r\n" + 
			    		"SITEID=A.SITEID AND\r\n" + 
			    		"INVUSENUM IN\r\n" + 
			    		"( SELECT INVUSENUM FROM INVUSE WHERE\r\n" + 
			    		"SITEID=A.SITEID AND STATUS IN\r\n" + 
			    		"(SELECT VALUE FROM SYNONYMDOMAIN WHERE\r\n" + 
			    		"DOMAINID = 'INVUSESTATUS' AND MAXVALUE IN ('STAGED', 'SHIPPED')) AND\r\n" + 
			    		"RECEIPTS IN\r\n" + 
			    		"(SELECT VALUE FROM SYNONYMDOMAIN\r\n" + 
			    		"WHERE DOMAINID='RECEIPTS' AND\r\n" + 
			    		"MAXVALUE IN ('NONE', 'PARTIAL')) AND\r\n" + 
			    		"INVUSELINEID IN ( SELECT INVUSELINEID FROM INVUSELINE WHERE RECEIVEDQTY = 0 ))))\r\n" + 
			    		"INNER JOIN ITEM IT\r\n" + 
			    		"ON IT.itemsetid=I.itemsetid and  I.itemnum=IT.itemnum and IT.itemtype in\r\n" + 
			    		"(select value from synonymdomain where domainid = 'ITEMTYPE'  and maxvalue = 'ITEM')" +
			    		"WHERE I.STATUS IN  ('ACTIVE','PLANNING')";
			PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
	
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\"+codeFileName+".csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{

				
				data.add(new String[]{rsData.getString("INVENTORYID")
						+"",""+rsData.getString("ITEMNUM")
						+ "",""+rsData.getString("ITEMSETID") + 
						"",""+rsData.getString("SITEID") + "",""+
						rsData.getString("LOCATION") + "",""+
						rsData.getString("STATUS") + "",""+
						rsData.getString("ISSUEUNIT") + "",""+
						rsData.getString("E3M_CURBAL") + "",""+
						rsData.getString("E3M_AVBAL")+ "",""+
					    rsData.getString("ASSETUID")
						});
			
				rCount=rCount+1;
			}
	        String[] header = "INVENTORYID,ITEMNUM,ITEMSETID,SITEID,LOCATION,STATUS,ISSUEUNIT,E3M_CURBAL,E3M_AVBAL,ASSETUID".split(",");	        
	        
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

