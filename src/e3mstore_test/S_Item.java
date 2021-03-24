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

public class S_Item {

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
		    String exQuery=" SELECT I.ITEMNUM,I.ITEMSETID,I.DESCRIPTION,I.ITEMTYPE,I.ITEMID,I.ROTATING,I.LOTTYPE,IL.IMGLIBID \r\n" + 
		    		"  FROM maximo.ITEM I\r\n" + 
		    		"  LEFT JOIN MAXIMO.IMGLIB IL\r\n" + 
		    		"  ON IL.refobject = 'ITEM' and IL.refobjectid = I.itemid\r\n" + 
		    		"  WHERE status  in ('ACTIVE', 'PLANNING') and \r\n" + 
		    		"  ITEMTYPE  in (select value from maximo.synonymdomain where domainid = 'ITEMTYPE'  and maxvalue in ('ITEM','TOOLS'))";
		
		    PreparedStatement exStmt = conn.prepareStatement(exQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			exStmt.setFetchSize(100);
			
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\ITEM.csv"); 
			
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
			ResultSet rsData=exStmt.executeQuery();
			while(rsData.next())
			{
				//System.out.println(rsData.getString("ASSETNUM"));
				if(rsData.getString("IMGLIBID")!=null )
				{		data.add(new String[]
						{rsData.getString("ITEMNUM")
						+ "",""+rsData.getString("ITEMSETID") + 
						"",""+rsData.getString("DESCRIPTION") + "",""+
						rsData.getString("ITEMTYPE") + "",""+	
						rsData.getString("ITEMID") + "",""+
						rsData.getString("ROTATING")+ "",""+
						rsData.getString("LOTTYPE")+ "",""+
						"http://maximo.eam360.com:9081/maximo/oslc/images/"+rsData.getString("IMGLIBID")
						});
				}
				else
				{
					data.add(new String[]
							{rsData.getString("ITEMNUM")
							+ "",""+rsData.getString("ITEMSETID") + 
							"",""+rsData.getString("DESCRIPTION") + "",""+
							rsData.getString("ITEMTYPE") + "",""+
							rsData.getString("ITEMID") + "",""+
							rsData.getString("ROTATING")+ "",""+
							rsData.getString("LOTTYPE")+ "",""+
							rsData.getString("IMGLIBID")
							});
					
				}	
						
			}
	        String[] header = "ITEMNUM,ITEMSETID,DESCRIPTION,ITEMTYPE,ITEMID,ROTATING,LOTTYPE,_IMAGELIBREF".split(",");	        
	        
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


	