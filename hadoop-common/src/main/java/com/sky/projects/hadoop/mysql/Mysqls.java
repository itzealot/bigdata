package com.sky.projects.hadoop.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.hadoop.common.Closeables;

public final class Mysqls {
	private static final Logger LOG = LoggerFactory.getLogger(Mysqls.class);

	// mysql config
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://192.168.0.112:3306/gacenter?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "surfilter1218";

	private static Connection conn = null;

	static {
		conn = getMysqlConnection(URL, USERNAME, PASSWORD);
	}

	public static Connection getMysqlConnection(String url, String username, String password) {
		try {
			Class.forName(MYSQL_DRIVER);
			return DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			LOG.error("get mysql connection failed. url={}, username={}, password={}, {}", url, username, password, e);
		}

		return null;
	}

	public static Connection getMysqlConnection(MysqlConfig config) {
		String url = config.getConnectionUrl();
		String username = config.getUserName();
		String password = config.getPassword();

		try {
			Class.forName(MYSQL_DRIVER);
			return DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			LOG.error("get mysql connection failed. url={}, username={}, password={}, {}", url, username, password, e);
		}

		return null;
	}

	public static int count(String tableName) {
		int value = 0;
		PreparedStatement pstm = null;
		ResultSet rs = null;

		try {
			pstm = conn.prepareStatement("SELECT count(*) FROM " + tableName);
			rs = pstm.executeQuery();

			if (rs.next()) {
				value = Integer.parseInt(rs.getString(1));
			}
		} catch (SQLException e) {
			LOG.error("count from Mysql's " + tableName + " error, {}", e);
		} finally {
			Closeables.close(rs, pstm);
		}

		return value;
	}

	public static void closeConnection() {
		Closeables.close(conn);
	}

	private Mysqls() {
	}
}
