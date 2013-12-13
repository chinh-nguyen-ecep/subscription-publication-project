package ecepvn.org.app;

import java.io.IOException;

import net.sf.jasperreports.engine.JRException;

import ecepvn.org.utils.JasperExporter;

public class jasperReportGenerater {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws JRException 
	 */
	public static void main(String[] args) throws IOException, JRException {
		// TODO Auto-generated method stub
		if(args.length==1){
			if(args[0].equals("--help")){
				System.out.println("input: [folder_content_jasper_files] [jasper file name] [export to folder] [export format] [export file name] [paramters]");
				System.out.println("Example: java -jar jasperReportGenerater.jar /opt/temp/reports abc /opt/temp xls jasperReport.2013-05-01 full_date=2013-05-01 publisher_id=2");
			}
			
		}else if(args.length>=5){
			String dir_folder=args[0];
			String jasper_file_name=args[1];
			String export_dir=args[2];
			String export_format=args[3];
			String export_file_name=args[4];
			System.out.println("Folder content jasper source: "+dir_folder);
			System.out.println("Jasper file name: "+jasper_file_name);
			System.out.println("Folder content export file: "+export_dir);
			System.out.println("Export file format: "+export_format);
			System.out.println("Export file name: "+export_file_name+"."+export_format);
			System.out.println("Input Params:");
			JasperExporter jasper=new JasperExporter();		
			jasper.setDirPath(dir_folder);
			jasper.setMainJasperFileName(jasper_file_name);
			jasper.setExportPath(export_dir);
			jasper.setExportFormat(export_format);
			jasper.setExportFileName(export_file_name);
			//Set params
			if(args.length>5){
				for(int i=5;i<args.length;i++){
					String[] array = args[i].split("=");
					System.out.println("Key:"+array[0]+" value: "+array[1]);
					jasper.addParams(array[0],array[1]);
				}				
			}
			jasper.reportGenerator();
		}
	}

}
