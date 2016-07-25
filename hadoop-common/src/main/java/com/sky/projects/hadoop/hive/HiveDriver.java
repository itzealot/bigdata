package com.sky.projects.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.sky.projects.hadoop.common.Closeables.close;

public final class HiveDriver {
	private static final Logger LOG = LoggerFactory.getLogger(HiveDriver.class);

	public static final String MERGE_SIZE = "1024000000";
	public static final String MERGE_SMALLFILES_SIZE = "256000000";
	public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	public static final String MYSQL_DRIVER = "com.mysql.Driver";

	public static Connection getHiveConnection(String url) {
		try {
			Class.forName(HIVE_DRIVER);
			return DriverManager.getConnection(url);
		} catch (Exception e) {
			LOG.error("get hive connection failed. url={}, {}", url, e);
		}

		return null;
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

	/**
	 * 刷新表
	 * 
	 * @param url
	 * @param tableName
	 * @return
	 */
	public static boolean refresh(String url, String tableName) {
		Connection conn = null;
		Statement stmt = null;
		String sql = "refresh " + tableName;
		boolean result = false;

		try {
			conn = getHiveConnection(url);
			stmt = conn.createStatement();
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("refresh table error,url={}, sql={}, {}", url, sql, e);
		} finally {
			close(stmt, conn);
		}

		return result;
	}

	/**
	 * 生成临时表
	 * 
	 * @param url
	 * @param tableName
	 * @param partition
	 * @param partitionValue
	 * @return
	 * @throws Exception
	 */
	public static boolean createTempTable(String url, String tableName, String partition, String partitionValue)
			throws Exception {
		boolean result = false;
		String tempTable = "temp_" + tableName + "_" + partitionValue;
		try {
			dropTable(url, tempTable);
		} catch (Exception e) {
		}

		String sql = "create table " + tempTable + " as select * from " + tableName + " where " + partition + "= '"
				+ partitionValue + "'";
		Connection conn = null;
		Statement stmt = null;

		try {
			conn = getHiveConnection(url);
			stmt = conn.createStatement();
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("create temp table error, sql={}, {}", sql, e);
		} finally {
			close(stmt, conn);
		}

		return result;
	}

	public static boolean dropTable(String url, String table) {
		boolean result = false;
		Connection conn = null;
		Statement stmt = null;
		String sql = "drop table " + table;

		try {
			conn = getHiveConnection(url);
			stmt = conn.createStatement();
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("drop table error, sql={}, {}", sql, e);
		} finally {
			close(stmt, conn);
		}

		return result;
	}

	/**
	 * 把表里的数据导入到后缀为_parquet的表；小文件合并
	 * 
	 * @param table
	 * @param partition
	 * @param partitionValue
	 */
	public boolean parquet(String hiveUrl, String table, String partition, String partitionValue,
			String parquetFileSize) throws Exception {
		String sql = builder(table, partition, partitionValue);
		Connection conn = null;
		Statement stmt = null;
		boolean result = false;

		try {
			conn = HiveDriver.getHiveConnection(hiveUrl);
			stmt = conn.createStatement();
			// stmt.execute("set hive.merge.size.per.task = 256*1000*1000");
			// stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
			stmt.execute("set parquet_file_size = " + parquetFileSize);
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("File transfer parquet format error, sql:{}, {}", sql, e);
		} finally {
			close(stmt, conn);
		}

		return result;
	}

	public boolean addPartition(String hiveUrl, String table, String partition, String partitionValue)
			throws Exception {
		String sql = "alter table " + table + " add partition(" + partition + "='" + partitionValue + "')";
		Connection conn = null;
		Statement stmt = null;

		try {
			conn = HiveDriver.getHiveConnection(hiveUrl);
			stmt = conn.createStatement();
			stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
			stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("add partition error, where url={}, sql={}, {}", hiveUrl, sql, e);
		} finally {
			close(stmt, conn);
		}

		return true;
	}

	/**
	 * 修改hive的表名称
	 */
	public boolean renamePartition(String hiveUrl, String table, String partition, String fromPartitionValue,
			String toPartitionValue) throws Exception {
		String sql = "alter table " + table + " partition(" + partition + "='" + fromPartitionValue
				+ "') rename to partition(" + partition + "='" + toPartitionValue + "')";
		Connection conn = null;
		Statement stmt = null;
		boolean result = false;

		try {
			conn = HiveDriver.getHiveConnection(hiveUrl);
			stmt = conn.createStatement();
			stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("rename partition error, sql={}, {}", sql, e);
		} finally {
			close(stmt, conn);
		}

		return result;
	}

	public static boolean dropPartition(String url, String table, String partition, String partitionValue) {
		boolean result = false;
		String sql = "alter table " + table + " drop partition(" + partition + " ='" + partitionValue + "')";
		Connection conn = null;
		Statement stmt = null;

		try {
			conn = getHiveConnection(url);
			stmt = conn.createStatement();
			result = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("drop partition error, url={}, sql={}, {}", url, sql, e);
		} finally {
			close(stmt, conn);
		}

		return result;
	}

	public static void dropPartitions(String url, String table, String partition, List<String> partitionValues) {
		Connection conn = null;
		PreparedStatement stmt = null;
		String sql = "alter table " + table + " drop partition(" + partition + "=?)";

		try {
			conn = getHiveConnection(url);
			stmt = conn.prepareStatement(sql);

			int len = partitionValues.size();
			for (int i = 0; i < len; i++) {
				stmt.setString(1, "'" + partitionValues.get(i) + "'");

				stmt.addBatch(sql);
			}

			stmt.executeBatch();
		} catch (Exception e) {
			LOG.error("drop partition error, url={}, sql={}, {}", url, sql, e);
		} finally {
			close(stmt, conn);
		}
	}

	/**
	 * 插入hive中的临时表
	 * 
	 * @param hiveUrl
	 * @param toTable
	 * @param partition
	 * @param partitionValue
	 * @return
	 */
	public boolean insert(String hiveUrl, String toTable, String columnValue, String partition, String partitionValue,
			String fromTable) throws Exception {
		String sql = builder(toTable, partition, partitionValue, fromTable, columnValue);
		boolean reuslt = false;
		Connection conn = null;
		Statement stmt = null;

		try {
			conn = HiveDriver.getHiveConnection(hiveUrl);
			stmt = conn.createStatement();
			/*
			 * hive合并文件推荐配置：hive.merge.smallfiles.avgsize=16000000
			 * hive.merge.size.per.task=256000000
			 */
			stmt.execute("set hive.merge.size.per.task = " + MERGE_SIZE);
			stmt.execute("set hive.merge.smallfiles.avgsize = " + MERGE_SMALLFILES_SIZE);
			stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
			reuslt = stmt.execute(sql);
		} catch (Exception e) {
			LOG.error("insert HiveTemp error, sql={}, {}", sql, e);
		} finally {
			close(stmt, conn);
		}

		return reuslt;
	}

	public boolean insertHiveTemp(String hiveUrl, String table, String columnValue, String partition,
			String partitionValue) throws Exception {
		return insert(hiveUrl, "temp_" + table + "_" + partitionValue, columnValue, "temp_" + partitionValue,
				partitionValue, table);
	}

	/**
	 * 获取表的所有字段（除去分区字段）及分区字段
	 * 
	 * @param hiveUrl
	 * @param table
	 * @return
	 * @throws Exception
	 */
	public Map<String, String> desc(String hiveUrl, String table) throws Exception {
		Map<String, String> returnMap = new HashMap<String, String>(2);
		String sql = "DESC " + table;
		StringBuffer buffer = new StringBuffer();
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String partitionColName = null;

		try {
			conn = HiveDriver.getHiveConnection(hiveUrl);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String fieldName = rs.getString(1);
				if (fieldName.endsWith("_p")) {
					partitionColName = fieldName;
					break;
				}

				buffer.append(",").append(fieldName);
			}
			returnMap.put("partition", partitionColName.toUpperCase());
			returnMap.put("cols", buffer.toString().substring(1).toUpperCase());
		} catch (Exception e) {
			LOG.error("desc table error, sql={}, {}", sql, e);
		} finally {
			close(rs, stmt, conn);
		}
		return returnMap;
	}

	private String builder(String table, String partition, String partitionValue) {
		return new StringBuffer().append("INSERT OVERWRITE TABLE ").append(table).append("_parquet")
				.append(" PARTITION(").append(partition).append(")").append(" SELECT * FROM ").append(table)
				.append(" WHERE ").append(partition).append("='").append(partitionValue).append("'").toString();
	}

	private String builder(String toTable, String partition, String partitionValue, String fromTable,
			String columnValue) {
		return new StringBuffer().append("INSERT OVERWRITE TABLE ").append(toTable).append(" PARTITION(")
				.append(partition).append("='").append(partitionValue).append("')")
				.append(" SELECT " + columnValue + " FROM ").append(fromTable).toString();
	}

	private HiveDriver() {
	}
}
