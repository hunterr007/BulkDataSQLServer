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

public class AssetContract_03 {

	public static void main(String[] args) {

		Connection conn = null;
		try {
			
			FileReader reader=new FileReader("C://Users//prashant//Documents//EclipseWorkspace//BulkDataSQLServer//Config.properties");  
			
			Properties prop=new Properties();  
			prop.load(reader);  
			
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
					prop.getProperty("bulkdata.db.password"));

			List<String[]> data = new ArrayList<String[]>();
			int rCount=0;
			String codeFileName="ASSETCONTRACT";
            
		    String exQuery="SELECT A.ASSETUID,C.CONTRACTTYPE, format(C.ENDDATE,'MM/dd/yyyy') \"ENDDATE\" FROM  ASSET A, CONTRACT C "
		    		+ "WHERE (A.STATUS NOT IN ('INACTIVE','DECOMMISSIONED')) "
		    		+ "AND C.CONTRACTTYPE='WARRANTY' AND C.ENDDATE IS NOT NULL AND "
		    		+ "(C.CONTRACTNUM IN (SELECT CONTRACTNUM FROM CONTRACTASSET  WHERE ASSETID = A.ASSETID) OR "
		    		+ "C.CONTRACTNUM IN (SELECT CONTRACTNUM FROM WARRANTYASSET  WHERE ASSETID = A.ASSETID)) "
		    		+ "ORDER BY A.ASSETID";
	

		    PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			

			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\"+codeFileName+".csv");
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				
				data.add(new String[]{rsData.getString("ASSETUID")
						+ "",""+rsData.getString("CONTRACTTYPE") 
						+ "",""+rsData.getString("ENDDATE")
						});
			
				rCount=rCount+1;
			}
	        String[] header = "ASSETUID,CONTRACTTYPE,ENDDATE".split(",");	        
	        
	        writer.writeNext(header);
	        writer.writeAll(data);
	        System.out.println("CSV Writing complete :" + rCount);
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


