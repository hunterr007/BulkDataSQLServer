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

public class LocationPW {

	public static void main(String[] args) {

		Connection conn = null;
		try {
			
			FileReader reader=new FileReader("Y://maxmobile//TEST//Storekeeper//Config.properties");   
			
			
			Properties prop=new Properties();  
			prop.load(reader);  
			int rCount=0;
			String codeFileName="LOCATIONS_PW";
			
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
				prop.getProperty("bulkdata.db.password"));


			List<String[]> data = new ArrayList<String[]>();

		     
		     String exQuery="SELECT T1.DESCRIPTION,T1.E3MSHRINKAGE,T1.LOCATION,COALESCE(T2.E3MHASASSETS,0)E3MHASASSETS,T1.PARENT,T1.TYPE,T1.SITEID,T1.HASCHILDREN,\r\n" + 
		     		"T1.LOCATIONSID,T1.PLUSSFEATURECLASS, T1.STATUS,T1.SADDRESSCODE,T3.FAILURECODE  FROM\r\n" + 
		     		"\r\n" + 
		     		"(SELECT L.DESCRIPTION, 0 AS E3MSHRINKAGE, L.LOCATION, L.E3MHASASSETS, LC.PARENT AS PARENT,L.TYPE, L.SITEID, LC.CHILDREN AS HASCHILDREN,\r\n" + 
		     		"L.LOCATIONSID,  L.PLUSSFEATURECLASS, L.STATUS,L.SADDRESSCODE\r\n" + 
		     		"FROM MAXIMO.LOCATIONS L , MAXIMO.LOCHIERARCHY LC\r\n" + 
		     		//"FROM MAXIMO.LOCHIERARCHY LC,MAXIMO.LOCATIONS L LEFT JOIN MAXIMO.LONGDESCRIPTION ON L.LOCATIONSID = LDKEY AND LDOWNERTABLE = 'LOCATIONS'\r\n" + 
		     		"WHERE  L.LOCATION=LC.LOCATION AND L.SITEID = LC.SITEID AND  L.SITEID='PW' AND LC.SITEID = 'PW' AND\r\n" + 
		     		"L.STATUS NOT IN (SELECT VALUE FROM MAXIMO.SYNONYMDOMAIN WHERE DOMAINID='LOCASSETSTATUS' AND MAXVALUE='DECOMMISSIONED')\r\n" + 
		     		"ORDER BY L.LOCATIONSID) T1\r\n" + 
		     		"\r\n" + 
		     		"LEFT JOIN\r\n" + 
		     		"(SELECT LOCATION, COALESCE (NULLIF (E3MHASASSETS,0),1) E3MHASASSETS,SITEID FROM  MAXIMO.LOCATIONS\r\n" + 
		     		"WHERE LOCATION IN (\r\n" + 
		     		"SELECT L.LOCATION FROM MAXIMO.LOCATIONS L, MAXIMO.ASSET A\r\n" + 
		     		"WHERE A.SITEID=L.SITEID AND A.LOCATION=L.LOCATION AND L.SITEID='PW' AND\r\n" + 
		     		"L.STATUS NOT IN (SELECT VALUE FROM MAXIMO.SYNONYMDOMAIN WHERE DOMAINID='LOCASSETSTATUS' AND MAXVALUE='DECOMMISSIONED') AND\r\n" + 
		     		"A.STATUS NOT IN (SELECT VALUE FROM MAXIMO.SYNONYMDOMAIN WHERE DOMAINID='LOCASSETSTATUS' AND MAXVALUE='DECOMMISSIONED')) AND SITEID = 'PW') T2\r\n" + 
		     		"ON T1.LOCATION = T2.LOCATION AND T1.SITEID=T2.SITEID\r\n" + 
		     		"\r\n" + 
		     		"LEFT JOIN \r\n" + 
		     		"\r\n" + 
		     		"(SELECT LOCATION,SITEID,FAILURECODE FROM MAXIMO.LOCOPER) T3\r\n" + 
		     		"ON T1.location=T3.location and T1.SITEID=T3.SITEID\r\n";
			
			PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\"+codeFileName+".csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
			
				
				data.add(new String[]{rsData.getString("DESCRIPTION")+ "",""+
						rsData.getString("E3MSHRINKAGE") +"",""+ 
						rsData.getString("LOCATION") +"",""+  
						rsData.getString("E3MHASASSETS") +"",""+  
						rsData.getString("PARENT") + "",""+
						rsData.getString("TYPE") + "",""+
						rsData.getString("SITEID") + "",""+
						rsData.getString("HASCHILDREN")  + "",""+ 
						rsData.getString("LOCATIONSID")+ "",""+
						//rsData.getString("PLUSSFEATURECLASS")+"",""+ 
						rsData.getString("STATUS")
						//rsData.getString("SADDRESSCODE")+"",""+ 
						//rsData.getString("FAILURECODE")
						//rsData.getString("LDTEXT") 
						
						
						});
			
				rCount=rCount+1;
			}
	        String[] header = "DESCRIPTION,E3MSHRINKAGE,LOCATION,E3MHASASSETS,PARENT,TYPE,SITEID,HASCHILDREN,LOCATIONSID,STATUS".split(",");	        
	        
	        writer.writeNext(header);
	        writer.writeAll(data);
	        System.out.println("CSV Writing complete "+ rCount);
	        writer.close(); 
	        
	       /* List<String[]> count = new ArrayList<String[]>();
	        File countFile = new File(prop.getProperty("bulkdata.file.destination")+"\\RecordCount.csv"); 
			FileWriter outputCountfile = new FileWriter(countFile,true); 	  
	        CSVWriter countWriter = new CSVWriter(outputCountfile); 
	        count.add(new String[]{codeFileName+ "",""+rCount});
	        countWriter.writeAll(count);
	        countWriter.close(); */
		    
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
