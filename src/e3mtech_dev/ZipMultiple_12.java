package e3mtech_dev;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipMultiple_12 {

	public static void main(String[] args) throws IOException {
		
		FileReader reader=new FileReader("C://Users//prashant//Documents//EclipseWorkspace//BulkDataSQLServer//Config.properties");  
		
		
		Properties prop=new Properties();  
		prop.load(reader);  
        
        String zipFile = prop.getProperty("bulkdata.zip.zipTarget")+"//EAM360DATA.zip";
         
        String[] srcFiles = {
        		prop.getProperty("bulkdata.zip.zipSrc")+"//ASSET.csv",
        		prop.getProperty("bulkdata.zip.zipSrc")+"//INVBALANCES.csv", 
        		prop.getProperty("bulkdata.zip.zipSrc")+"//INVCOST.csv", 
        		prop.getProperty("bulkdata.zip.zipSrc")+"//INVENTORY.csv", 
        		prop.getProperty("bulkdata.zip.zipSrc")+"//ITEM.csv", 
        		prop.getProperty("bulkdata.zip.zipSrc")+"//ITEMCONDITION.csv",
        		prop.getProperty("bulkdata.zip.zipSrc")+"//LOCATION.csv",  
        		prop.getProperty("bulkdata.zip.zipSrc")+"//SERVICEADDRESS.csv",
        		prop.getProperty("bulkdata.zip.zipSrc")+"//ASSETCONTRACT.csv",
        		prop.getProperty("bulkdata.zip.zipSrc")+"//DateFile.csv",
        		prop.getProperty("bulkdata.zip.zipSrc")+"//RecordCount.csv"
        		};
        try {
             
            byte[] buffer = new byte[1024];
            FileOutputStream fileOs = new FileOutputStream(zipFile);
            ZipOutputStream zipOs = new ZipOutputStream(fileOs);
             
            for (int i=0; i < srcFiles.length; i++) {
                File srcFile = new File(srcFiles[i]);
                FileInputStream fips = new FileInputStream(srcFile);
                zipOs.putNextEntry(new ZipEntry(srcFile.getName()));
                 
                int length;
                while ((length = fips.read(buffer)) > 0) {
                	zipOs.write(buffer, 0, length);
                }
 
                zipOs.closeEntry();
                fips.close();    
            }
            System.out.println("Zipping successful");
            zipOs.close();          
        }
        catch (IOException ioe) {
            System.out.println("Error creating zip file: " + ioe);
        }
         
    }
 
}

