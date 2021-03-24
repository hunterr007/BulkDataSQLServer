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

public class InvBalances_07 {

	public static void main(String[] args) {
		
		Connection conn = null;
		try {
			
			FileReader reader=new FileReader("C://Users//prashant//Documents//EclipseWorkspace//BulkDataSQLServer//Config.properties");  
			
			Properties prop=new Properties();  
			prop.load(reader);  
			int rCount=0;
			String codeFileName="INVBALANCES";
			
			Class.forName(prop.getProperty("bulkdata.db.driver"));
			String connectionString = prop.getProperty("bulkdata.db.connectionString");
	        
			conn = DriverManager.getConnection(connectionString,prop.getProperty("bulkdata.db.username"),
					prop.getProperty("bulkdata.db.password"));

			List<String[]> data = new ArrayList<String[]>();
		    String exQuery="SELECT I.INVBALANCESID,I.ITEMNUM,I.ITEMSETID,I.PHYSCNTDATE,I.RECONCILED,I.SITEID,I.CURBAL,"
		    		+ "I.ORGID,I.BINNUM,I.CONDITIONCODE,I.LOTNUM,I.LOCATION,IT.DESCRIPTION,I.PHYSCNT,INV.ABCTYPE FROM"
		    		+ " INVBALANCES I LEFT JOIN ITEM IT ON IT.ITEMNUM = I.ITEMNUM AND IT.ITEMSETID = I.ITEMSETID"
		    		+ " LEFT JOIN INVENTORY INV ON I.ITEMNUM = INV.ITEMNUM AND I.LOCATION = INV.LOCATION AND"
		    		+ " I.ITEMSETID = INV.ITEMSETID AND I.SITEID = INV.SITEID";
		    PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\"+codeFileName+".csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        

			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				
				
				data.add(new String[]{rsData.getString("INVBALANCESID")
						+ "",""+rsData.getString("ITEMNUM")
						+ "",""+rsData.getString("ITEMSETID")
						+ "",""+rsData.getString("SITEID") + 
						"",""+rsData.getString("CURBAL") + "",""+
						rsData.getString("BINNUM") + "",""+
						rsData.getString("LOTNUM")+ "",""+
						rsData.getString("LOCATION")
						});
				rCount=rCount+1;
		
			}
	        String[] header = "INVBALANCESID,ITEMNUM,ITEMSETID,SITEID,CURBAL,BINNUM,LOTNUM,LOCATION".split(",");	        
	        
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
