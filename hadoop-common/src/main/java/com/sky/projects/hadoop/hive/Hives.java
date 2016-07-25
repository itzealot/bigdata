package com.sky.projects.hadoop.hive;

import static com.sky.projects.hadoop.common.Closeables.close;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.hadoop.common.SqlBuilder;

/**
 * Hive的JavaApi
 * 
 * 启动hive的远程服务接口命令行执行：hive --service hiveserver >/dev/null 2>/dev/null &
 * 
 * @author zt
 */
public final class Hives {
	private static final Logger LOG = LoggerFactory.getLogger(Hives.class);
	public static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public static long count(Statement stmt, String tableName) {
		ResultSet res = null;

		try {
			res = stmt.executeQuery(SqlBuilder.count(tableName));

			return res.next() ? Long.parseLong(res.getString(1)) : 0L;
		} catch (Exception e) {
			LOG.error("count table={} error, {}", tableName, e);
			return 0L;
		} finally {
			close(res);
		}
	}

	public static boolean loadData(Statement stmt, String localPath, String tableName) {
		try {
			return stmt.executeUpdate("load data local inpath '" + localPath + "' into table " + tableName) != 0;
		} catch (SQLException e) {
			LOG.error("load data from local into table error, localPath={}, tableName={}", localPath, tableName, e);
			return false;
		}
	}

	public static List<TableDesc> desc(Statement stmt, String tableName) {
		return describe(stmt, tableName);
	}

	public static List<TableDesc> describe(Statement stmt, String tableName) {
		List<TableDesc> results = new ArrayList<>();
		ResultSet res = null;

		try {
			res = stmt.executeQuery("describe " + tableName);
			while (res.next()) {
				results.add(new TableDesc(res.getString(1), res.getString(2)));
			}
		} catch (SQLException e) {
			LOG.error("describe table error where table={}, {}", tableName, e);
		} finally {
			close(res);
		}

		return results;
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
			LOG.error("show tables error, table={}, {}", tableName, e);
		} finally {
			close(res);
		}

		return buffer.toString();
	}

	/**
	 * create table sql like: create table tableName (key int, value string) row
	 * format delimited fields terminated by '\t'
	 * 
	 * @param stmt
	 * @param createTableSql
	 * @return
	 */
	public static boolean createTable(Statement stmt, String createTableSql) {
		try {
			return stmt.executeUpdate(createTableSql) != 0;
		} catch (SQLException e) {
			LOG.error("create table error, create table sql={}, {}", createTableSql, e);
			return false;
		}
	}

	public static boolean dropTable(Statement stmt, String tableName) {
		try {
			return stmt.executeUpdate("drop table " + tableName) != 0;
		} catch (SQLException e) {
			LOG.error("drop table error, table={}, {}", tableName, e);
			return false;
		}
	}
}