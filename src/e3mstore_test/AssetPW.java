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

public class AssetPW {
	public static void main(String[] args) {

		Connection conn = null;
		try {
			
			FileReader reader=new FileReader("Y://maxmobile//TEST//Storekeeper//Config.properties");
			
			Properties prop=new Properties();  
			prop.load(reader); 
			int rCount=0;
			String codeFileName="ASSET_PW";
			
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
					prop.getProperty("bulkdata.db.password"));
			
			List<String[]> data = new ArrayList<String[]>();
		    String exQuery="SELECT ASSETNUM,ISRUNNING, LOCATION, DESCRIPTION,STATUS, SITEID, ASSETID, ASSETUID, BINNUM,ITEMNUM\r\n" + 
		    		"FROM MAXIMO.ASSET WHERE  SITEID='PW' AND STATUS NOT IN\r\n" + 
		    		"(SELECT VALUE FROM MAXIMO.SYNONYMDOMAIN WHERE DOMAINID='LOCASSETSTATUS' AND MAXVALUE='DECOMMISSIONED') ORDER BY ASSETUID"; 
           

		    PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\"+codeFileName+".csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				data.add(new String[]{rsData.getString("ASSETNUM")+ "",""+
						rsData.getString("ISRUNNING")+ "",""+
						rsData.getString("LOCATION")+ "",""+ 
						rsData.getString("DESCRIPTION") +"",""+ 
						rsData.getString("STATUS") + "",""+
						rsData.getString("SITEID") + "",""+
						rsData.getString("ASSETID") + "",""+
						rsData.getString("ASSETUID") + "",""+
						//rsData.getString("PLUSSFEATURECLASS") + "",""+
						rsData.getString("BINNUM")  + "",""+
						rsData.getString("ITEMNUM") 
						//rsData.getString("FAILURECODE") 
						//rsData.getString("LDTEXT") 
						});
				rCount=rCount+1;
		
			}
	        String[] header = "ASSETNUM,ISRUNNING,LOCATION,DESCRIPTION,STATUS,SITEID,ASSETID,ASSETUID,BINNUM,ITEMNUM".split(",");	        
	        
	        writer.writeNext(header);
	        writer.writeAll(data);
	        System.out.println("CSV Writing complete "+ rCount);
	        writer.close(); 
	        
	      /*  List<String[]> count = new ArrayList<String[]>();
	        File countFile = new File(prop.getProperty("bulkdata.file.destination")+"\\RecordCount.csv"); 
			FileWriter outputCountfile = new FileWriter(countFile,true); 	  
	        CSVWriter countWriter = new CSVWriter(outputCountfile); 
	        count.add(new String[]{codeFileName+ "",""+rCount});
	        countWriter.writeAll(count);
	        countWriter.close();*/
		    
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
