package e3mtech_dev;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVWriter;

public class GeneratedDateFile_11 {

	public static void main(String[] args) {

		try {
			
			FileReader reader=new FileReader("C://Users//prashant//Documents//EclipseWorkspace//BulkDataSQLServer//Config.properties");  
			
			Properties prop=new Properties();  
			prop.load(reader);  
			
			List<String[]> data = new ArrayList<String[]>();
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\DateFile.csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
	        CSVWriter writer = new CSVWriter(outputfile); 
	        
	        
	        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ");
	        Date today = Calendar.getInstance().getTime();        
	        String date= dateFormat .format(today);
	      
	        data.add(new String[] {date});
	        writer.writeAll(data);
	        System.out.println("CSV Writing complete"+ date);
	        writer.close(); 
	    
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
        
	}

}
