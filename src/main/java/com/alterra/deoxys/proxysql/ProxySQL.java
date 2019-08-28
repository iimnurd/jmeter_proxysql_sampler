package com.alterra.deoxys.proxysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.mariadb.jdbc.Driver;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.Date;

/**
 * @author IIM NUR DIANSYAH @ alterra
 * Aug 27, 2019
 */
public class ProxySQL {
	Driver driver = new Driver();
	Properties props = new Properties();
	ResultSet rs;
	String variable_value;
	Connection conn = null;
	String JDBC_URL = "";

	
	public Connection buildConnection(String host, String port, String username, String password) {

		JDBC_URL = "jdbc:mysql://address=(protocol=tcp)(host=" + host + ")(port=" + port + ")";

		props.put("user", username);
		props.put("password", password);

		System.out.println("\n------------ MariaDB Connector/J and ProxySQL Testing ------------\n");

		System.out.println("Trying connection...");
		try {
			conn = driver.connect(JDBC_URL, props);

		} catch (SQLException e) {
			System.out.println("Connection Failed!");
			System.out.println("Error cause: " + e.getCause());
			System.out.println("Error message: " + e.getMessage());
			return null;
		}
		return conn;

	}

	public String getStringLog(String host, String port, String username, String password) {
		conn = this.buildConnection(host, port, username, password);
		try {
			rs = conn.createStatement().executeQuery("select *from stats_mysql_query_digest;");
			while (rs.next()) {
				variable_value += rs.getString("digest_text") + "\n";
				// System.out.println("variable_value : " + variable_value);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return variable_value;
	}

	public JSONArray getAllDigestJson(String host, String port, String username, String password, String query)
			throws SQLException, IOException {
		conn = this.buildConnection(host, port, username, password);
		ResultSet resultSet = conn.createStatement().executeQuery(query);
		JSONArray json = new JSONArray();
		ResultSetMetaData metadata = resultSet.getMetaData();
		int numColumns = metadata.getColumnCount();

		while (resultSet.next()) {
			JSONObject obj = new JSONObject();
			for (int i = 1; i <= numColumns; ++i) {
				String column_name = metadata.getColumnName(i);
				obj.put(column_name, resultSet.getObject(column_name));
				obj.put("first_seen_human",
						this.convertTimestampToHuman(Long.parseLong(resultSet.getString("first_seen"))));
				obj.put("last_seen_human",
						this.convertTimestampToHuman(Long.parseLong(resultSet.getString("last_seen"))));

			}
			json.add(obj);
		}

		return json;
	}

	public JSONArray getAllSummaryGlobalJson(String host, String port, String username, String password, String query)
			throws SQLException, IOException {
		conn = this.buildConnection(host, port, username, password);
		ResultSet resultSet = conn.createStatement().executeQuery(query);
		JSONArray json = new JSONArray();
		ResultSetMetaData metadata = resultSet.getMetaData();
		int numColumns = metadata.getColumnCount();

		while (resultSet.next()) {
			JSONObject obj = new JSONObject();
			for (int i = 1; i <= numColumns; ++i) {
				String column_name = metadata.getColumnName(i);
				obj.put(column_name, resultSet.getObject(column_name));

			}
			json.add(obj);
		}

		return json;
	}

	public JSONArray getAllSummarySlowQueryJson(String host, String port, String username, String password,
			String query) throws SQLException, IOException {
		conn = this.buildConnection(host, port, username, password);
		ResultSet resultSet = conn.createStatement().executeQuery(query);

		JSONArray json = new JSONArray();
		ResultSetMetaData metadata = resultSet.getMetaData();
		int numColumns = metadata.getColumnCount();

		while (resultSet.next()) {
			JSONObject obj = new JSONObject();
			for (int i = 1; i <= numColumns; ++i) {
				String column_name = metadata.getColumnName(i);
				obj.put(column_name, resultSet.getObject(column_name));

			}
			json.add(obj);
		}

		return json;
	}

	public long getLongQueryTime(String host, String port, String username, String password)
			throws SQLException, IOException {
		conn = this.buildConnection(host, port, username, password);
		String query = "select variable_value from global_variables where variable_name='mysql-long_query_time'";
		ResultSet resultSet = conn.createStatement().executeQuery(query);
		long val = 0;

		while (resultSet.next()) {
			val = resultSet.getLong("variable_value");
		}

		return val;
	}

	public JSONArray getAllQueryCounterJson(String host, String port, String username, String password, String query)
			throws SQLException, IOException {
		conn = this.buildConnection(host, port, username, password);
		ResultSet resultSet = conn.createStatement().executeQuery(query);
		JSONArray json = new JSONArray();
		ResultSetMetaData metadata = resultSet.getMetaData();
		int numColumns = metadata.getColumnCount();

		while (resultSet.next()) {
			JSONObject obj = new JSONObject();
			for (int i = 1; i <= numColumns; ++i) {
				String column_name = metadata.getColumnName(i);
				obj.put(column_name, resultSet.getObject(column_name));

			}
			json.add(obj);
		}

		return json;
	}

	public String beautifyJson(String json) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(json);
		String prettyJsonString = gson.toJson(je);

		return prettyJsonString;
	}

	public String convertTimestampToHuman(long unixSeconds) {

		Date date = new java.util.Date(unixSeconds * 1000L);
		// the format of your date
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		// give a timezone reference for formatting (see comment at the bottom)
		sdf.setTimeZone(java.util.TimeZone.getDefault());
		String formattedDate = sdf.format(date);

		return formattedDate;
	}

}
