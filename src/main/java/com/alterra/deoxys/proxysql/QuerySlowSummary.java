package com.alterra.deoxys.proxysql;





import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.json.simple.JSONArray;



/**
 * @author IIM NUR DIANSYAH @ alterra
 * Aug 27, 2019
 */
public class QuerySlowSummary extends AbstractJavaSamplerClient  {

	
	String host = "127.0.0.1";
	String port = "16032";
	String databaseName = "";
	String username = "newuser";
	String password = "password";
	String field = "*";
	String MYSQL_URL = "";
	String driver ="com.mysql.jdbc.Driver";
	String limit = "10";
	String order_by = "average_time_ms desc";
	
	
	@Override
	public Arguments getDefaultParameters() {
		
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument("host", "");
		defaultParameters.addArgument("port", "");
		defaultParameters.addArgument("database", "");
		defaultParameters.addArgument("username", "");
		defaultParameters.addArgument("password", "");
		defaultParameters.addArgument("order_by", "average_time_ms desc");
		defaultParameters.addArgument("limit", "");
		return defaultParameters;
	}
	
	

	
	//get variable from jmeter
	@Override 
	public void setupTest(JavaSamplerContext context) {
		Logger logger   = Logger.getLogger( QuerySlowSummary.class.getName()); 
	    logger.info("sample setup" ); 
		this.host = context.getParameter("host");
		this.port = context.getParameter("port");
		this.databaseName = context.getParameter("database");
		this.username = context.getParameter("username");
		this.password = context.getParameter("password");
		this.field = context.getParameter("order_by");
		this.limit = context.getParameter("limit");
		//create logger
		logger.info("HOST : " + host +"\n");
		logger.info("PORT : " + port +"\n");
		logger.info("DATABASE : " + databaseName +"\n");
		
		
		
		super.setupTest(context);
	}



	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		
		
		SampleResult sampleResult = new SampleResult();
		boolean success = true;
		String response = "";
		sampleResult.sampleStart(); 
		String query ="";
		ProxySQL ps2 = new ProxySQL();
		
		long queryTime=0;
		try {
			queryTime = ps2.getLongQueryTime( this.host, this.port, this.username, this.password);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		if(this.databaseName == "") {
			 query = "select hostgroup, schemaname,first_seen, last_seen, count_star,sum_time,(sum_time/count_star)/1000 as average_time_ms,digest_text  from stats_mysql_query_digest  where average_time_ms > "+  String.valueOf(queryTime) +" order by "+ this.order_by + " limit " + this.limit;
		}else {
			
			 query = "select hostgroup, schemaname,first_seen, last_seen, count_star,sum_time,(sum_time/count_star)/1000 as average_time_ms,digest_text  from stats_mysql_query_digest  where average_time_ms > "+String.valueOf(queryTime) +" and schemaname='"+ this.databaseName+"' order by "+ this.order_by + " limit " + this.limit;
		}
		
		Logger logger   = Logger.getLogger( QuerySlowSummary.class.getName()); 
		
		
		try {
			
			logger.info("HOST : " + host +"\n");
			logger.info("PORT : " + port +"\n");
			logger.info("USERNAME : " + username +"\n");
			logger.info("PASSWORD : " + password +"\n");
			logger.info("QUERY : " + query +"\n");
			JSONArray cc = ps2.getAllDigestJson(this.host, this.port, this.username, this.password, query);
			String data = ps2.beautifyJson(cc.toString());
			
			sampleResult.sampleEnd();
			sampleResult.setSuccessful(success);
			sampleResult.setResponseData(data);
			sampleResult.setResponseMessage("Success");
			sampleResult.setResponseCodeOK(); // 200 code
		} catch (Exception e) {
			sampleResult.sampleEnd(); // stop stopwatch
			sampleResult.setSuccessful(false);
			sampleResult.setResponseMessage("Exception: " + e);
			
			// get stack trace as a String to return as document data
			java.io.StringWriter stringWriter = new java.io.StringWriter();
			e.printStackTrace(new java.io.PrintWriter(stringWriter));
			sampleResult.setResponseData(stringWriter.toString().getBytes());
			sampleResult.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
			sampleResult.setResponseCode("500");
		}
		
		
		
		return sampleResult;
	}
//	
	@Override
	public void teardownTest(JavaSamplerContext context) {
		try {
			
		} catch (Exception e) {
			e.printStackTrace();
		}
				super.teardownTest(context);
	}
}

