package e3mstore_test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.nio.file.Path; 
import java.nio.file.Paths;
import au.com.bytecode.opencsv.CSVWriter;



public class PrepCountCSV {

	public static void main(String[] args) throws IOException {
		
			
			FileReader reader=new FileReader("Y://maxmobile//TEST//STOREKEEPER//Config.properties");  
			Properties prop=new Properties();  
			prop.load(reader);  
			
			File file = new File(prop.getProperty("bulkdata.file.destination")+"\\RecordCount.csv"); 
			FileWriter outputfile = new FileWriter(file); 	  
			CSVWriter writer = new CSVWriter(outputfile); 
			
			String[] srcFiles = {
	        		prop.getProperty("bulkdata.file.destination")+"//ASSET.csv", 
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//INVBALANCES.csv", 
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//INVCOST.csv", 
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//INVENTORY.csv", 
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//ITEM.csv", 
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//ITEMCONDITION.csv",
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//ASSET_DUS.csv",
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//ASSET_PW.csv",
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//LOCATIONS_PW.csv",
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//LOCATIONS_DUS.csv",
	        		prop.getProperty("bulkdata.zip.zipSrc")+"//DateFile.csv"
	        		};
			
			int fileCount=11;
			String[] header = "FILE,COUNT".split(",");	 
			writer.writeNext(header);
			int noOfLines=0;
			List<String[]> data = new ArrayList<String[]>();
			for(int i=0;i<fileCount;i++)
			{	noOfLines=0;
				 try (BufferedReader freader = new BufferedReader(new FileReader(srcFiles[i])))
				 {
				        while (freader.readLine() != null) 
				        {
				            noOfLines++;
				        }
			  Path path = Paths.get(srcFiles[i]);
			  Path fileName= path.getFileName();
			  if(noOfLines>0)
			  {noOfLines=noOfLines-1;}
			  String rowCount=String.valueOf(noOfLines);
			  data.add(new String[]
					  {fileName.toString() + "",""+rowCount
					  });
			  
			     } 
			
			}
			writer.writeAll(data);
			
			System.out.println("RecordCount.CSV Writing complete");
			
			writer.close(); 
			}
	}


