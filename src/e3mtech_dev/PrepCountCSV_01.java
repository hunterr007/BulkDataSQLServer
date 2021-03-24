package e3mtech_dev;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVWriter;

public class PrepCountCSV_01 {

	public static void main(String[] args) throws IOException {
		
			
			FileReader reader=new FileReader("C://Users//prashant//Documents//EclipseWorkspace//BulkDataSQLServer//Config.properties");  
			Properties prop=new Properties();  
			prop.load(reader);  
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\RecordCount.csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
			CSVWriter writer = new CSVWriter(outputfile); 
			
			String[] header = "FILE,COUNT".split(",");	 
			writer.writeNext(header);	
			
			System.out.println("Blank RecordCount.CSV Writing complete");
			
			writer.close(); 
	}

}
