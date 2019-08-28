package com.alterra.deoxys.proxysql;



import java.util.ArrayList;
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
public class QueryCommandCounters extends AbstractJavaSamplerClient  {

	
	String host = "127.0.0.1";
	String port = "16032";
	String databaseName = "";
	String username = "newuser";
	String password = "password";
	String field = "*";
	String MYSQL_URL = "";
	String driver ="com.mysql.jdbc.Driver";
	String limit = "10";
	String order_by = "Total_cnt desc";
	
	
	@Override
	public Arguments getDefaultParameters() {
		
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument("host", "");
		defaultParameters.addArgument("port", "");
		defaultParameters.addArgument("username", "");
		defaultParameters.addArgument("password", "");
		defaultParameters.addArgument("order_by", "Total_cnt desc");
		defaultParameters.addArgument("limit", "");
		return defaultParameters;
	}
	
	

	
	//get variable from jmeter
	@Override 
	public void setupTest(JavaSamplerContext context) {
		Logger logger   = Logger.getLogger( QueryCommandCounters.class.getName()); 
	    logger.info("sample setup" ); 
		this.host = context.getParameter("host");
		this.port = context.getParameter("port");
		this.username = context.getParameter("username");
		this.password = context.getParameter("password");
		this.field = context.getParameter("order_by");
		this.limit = context.getParameter("limit");
		//create logger
		logger.info("HOST : " + host +"\n");
		logger.info("PORT : " + port +"\n");
		
		
		
		
		super.setupTest(context);
	}



	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		
		
		SampleResult sampleResult = new SampleResult();
		boolean success = true;
		String response = "";
		sampleResult.sampleStart(); 
		String query ="";
		query = "select *from stats_mysql_commands_counters  order by "+ this.order_by + " limit " + this.limit;
		
		
		Logger logger   = Logger.getLogger( QueryCommandCounters.class.getName()); 
		//String classpath = System.getProperty("java.class.path");
		//System.out.println(classpath);
		
	//	logger.info("classpath :"+classpath);
		try {
			ProxySQL ps2 = new ProxySQL();
			logger.info("HOST : " + host +"\n");
			logger.info("PORT : " + port +"\n");
			logger.info("USERNAME : " + username +"\n");
			logger.info("PASSWORD : " + password +"\n");
			logger.info("QUERY : " + query +"\n");
			JSONArray jsArray = ps2.getAllQueryCounterJson(this.host, this.port, this.username, this.password, query);
			String data = ps2.beautifyJson(jsArray.toString());
			
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

