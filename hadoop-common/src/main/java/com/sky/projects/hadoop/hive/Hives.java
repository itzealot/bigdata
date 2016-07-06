package com.sky.projects.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.sky.projects.hadoop.common.SqlBuilder;

/**
 * Hive的JavaApi
 * 
 * 启动hive的远程服务接口命令行执行：hive --service hiveserver >/dev/null 2>/dev/null &
 * 
 * @author zt
 */
public final class Hives {
	public static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static final Logger LOG = Logger.getLogger(Hives.class);

	public static long count(Statement stmt, String tableName) {
		String sql = SqlBuilder.count(tableName);
		long value = 0L;
		ResultSet res = null;

		try {
			res = stmt.executeQuery(sql);

			if (res.next()) {
				value = Long.parseLong(res.getString(1));
			}
		} catch (Exception e) {
			LOG.error("execute sql=" + sql + " count exception", e);
		} finally {
			close(res);
		}

		return value;
	}

	public static ResultSet query(Statement stmt, String tableName) throws SQLException {
		String sql = "SELECT * FROM " + tableName;

		return stmt.executeQuery(sql);
	}

	public static void loadData(Statement stmt, String tableName, String filePath) {
		String sql = "load data local inpath '" + filePath + "' into table " + tableName;

		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
		}

	}

	public static String desc(Statement stmt, String tableName) {
		String sql = "describe " + tableName;
		StringBuffer buffer = new StringBuffer();
		ResultSet res = null;

		try {
			res = stmt.executeQuery(sql);

			while (res.next()) {
				buffer.append(res.getString(1));
				buffer.append("\t");
				buffer.append(res.getString(2));
			}
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
		} finally {
			close(res);
		}

		return buffer.toString();
	}

	public static String show(Statement stmt, String tableName) {
		String sql = "show tables '" + tableName + "'";

		StringBuffer buffer = new StringBuffer();
		ResultSet res = null;

		try {
			res = stmt.executeQuery(sql);
			if (res.next()) {
				buffer.append(res.getString(1));
			}
		} catch (SQLException e) {
			LOG.error("show tables error, sql is " + sql, e);
		} finally {
			close(res);
		}

		return buffer.toString();
	}

	public static boolean createTable(Statement stmt, String tableName) {
		String sql = "create table " + tableName
				+ " (key int, value string)  row format delimited fields terminated by '\t'";

		int value = 0;

		try {
			value = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
		}

		return value != 0;
	}

	public static boolean dropTable(Statement stmt, String tableName) {
		String sql = "drop table " + tableName;

		int value = 0;
		try {
			value = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return value != 0;
	}

	public static Connection getConn(String url, String user, String password) {
		Connection conn = null;

		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			LOG.error("get driver error where url is " + url + ", user is " + user + ", password is " + password, e);
			throw new IllegalArgumentException("hive get connection exception in Hives.getConn", e);
		}

		return conn;
	}

	public static void close(AutoCloseable... clos) {
		if (clos == null) {
			return;
		}

		for (AutoCloseable clo : clos) {
			if (clo != null) {
				try {
					clo.close();
					clo = null;
				} catch (Exception e) {
					LOG.error("close connection failed!", e);
				}
			}
		}

	}
}