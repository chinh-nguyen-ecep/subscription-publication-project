package ecepvn.org.utils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;

import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRExporterParameter;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.export.JRCsvExporter;
import net.sf.jasperreports.engine.export.JRPdfExporter;
import net.sf.jasperreports.engine.export.JRXlsExporter;


public class JasperExporter {

	/**
	 * @param args
	 */
	private String dirPath="";
	private String mainJasperFileName ="";
	private String exportPath="";
	private String exportFormat="pdf";
	private String exportFileName="";
	private HashMap params=new HashMap();
	
	public JasperExporter() {
		super();
		
	}

	public void setExportFileName(String exportFileName) {
		this.exportFileName = exportFileName;
	}

	public void setMainJasperFileName(String mainJasperFileName) {
		this.mainJasperFileName = mainJasperFileName;
	}

	public void setParams(HashMap params) {
		this.params = params;
	}

	public void addParams(String key,String value) {
		this.params.put(key, value);
	}

	public String getDirPath() {
		return dirPath;
	}

	public void setDirPath(String dirPath) {
		this.dirPath = dirPath;
	}

	public String getMainJasperFileName() {
		return mainJasperFileName;
	}

	public String getExportPath() {
		return exportPath;
	}

	public String getExportFormat() {
		return exportFormat;
	}

	public String getExportFileName() {
		return exportFileName;
	}

	public void setExportPath(String exportPath) {
		this.exportPath = exportPath;
	}

	
	public void setExportFormat(String exportFormat) {
		this.exportFormat = exportFormat;
	}

	public void reportGenerator () throws IOException, JRException{
		JasperReport jasperReport = null;
		JasperPrint jasperPrint = null;
		// compile jasper report files
		
			System.setProperty( "jasper.reports.compiler.class", "net.sf.jasperreports.compilers.JRGroovyCompiler" );
			//JasperCompileManager.compileReportToFile( this.dirPath+"//"+this.mainJasperFileName+".jrxml",this.dirPath+"//"+this.mainJasperFileName+".jasper" );
			jasperReport =JasperCompileManager.compileReport(this.dirPath+"//"+this.mainJasperFileName+".jrxml");
			
		
		//get connection
		Connection conn=ConnectDB.getConnection();
		
		// Generate jasper print
		params.put("SUBREPORT_DIR", this.dirPath+"//");
		
			jasperPrint = JasperFillManager.fillReport(jasperReport, this.params, conn);
		
		//Check if export path do not exit
		File file = new File(this.exportPath);
		if(file.exists()){
		    //System.out.println("File Exists");
		}else{
		    boolean wasDirecotyMade = file.mkdirs();
		    if(wasDirecotyMade)System.out.println("Direcoty Created");
		    else System.out.println("Sorry could not create directory");
		}
		
		if(this.exportFileName.equals("")){
			this.exportFileName=this.mainJasperFileName;
		}
		
		// 1 - Export csv
		if(this.exportFormat.toLowerCase().equals("csv")){
			JRCsvExporter exporterCsv=new JRCsvExporter();
			exporterCsv.setParameter(JRExporterParameter.JASPER_PRINT, jasperPrint);
			exporterCsv.setParameter(JRExporterParameter.OUTPUT_FILE_NAME,  this.exportPath+"//"+this.exportFileName+".csv" );	
			exporterCsv.exportReport();
		}

		// 2- export to Excel sheet
		else if(this.exportFormat.toLowerCase().equals("xls")){
			JRXlsExporter exporter = new JRXlsExporter();
			exporter.setParameter(JRExporterParameter.JASPER_PRINT, jasperPrint);
			exporter.setParameter(JRExporterParameter.OUTPUT_FILE_NAME,  this.exportPath+"//"+this.exportFileName+".xls" );
			exporter.exportReport();
		}else{
			// 3- Export pdf file
			JRPdfExporter exporterPdf=new JRPdfExporter();
			exporterPdf.setParameter(JRExporterParameter.JASPER_PRINT, jasperPrint);
			exporterPdf.setParameter(JRExporterParameter.OUTPUT_FILE_NAME,  this.exportPath+"//"+this.exportFileName+".pdf" );	
			exporterPdf.exportReport();
		}
		
		System.out.println("Jasper exported - Done");
		
	}

	public static void main(String[] args) throws IOException, JRException {
		// TODO Auto-generated method stub
		JasperExporter jasper=new JasperExporter();		
		jasper.setDirPath("D:\\Ecep\\Document-Tailieu\\Pentaho\\biserver-ce-3.5\\pentaho-solutions\\Adcel reports\\jaspers\\daily_ad_serving_statistics_summary");
		jasper.setMainJasperFileName("daily_ad_serving_statistics_summary");
		jasper.setExportPath("D:\\testReport");
		jasper.addParams("eastern_date_sk", "3080");
		jasper.setExportFormat("xls");
		jasper.setExportFileName("daily_ad_serving_statistics_summary.2013-06-09");
		
		jasper.reportGenerator();
		
	}

}
