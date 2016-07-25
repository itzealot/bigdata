package com.sky.projects.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.hadoop.common.Closeables;

/**
 * HBase 基本操作
 * 
 * HBaseAdmin ---> Admin
 * 
 * HTableDescriptor ---> Table
 * 
 * HColumnDescriptor ---> ColumnFamily
 * 
 * 
 * @author zt
 */
public final class Hbase {
	private static final Logger LOG = LoggerFactory.getLogger(Hbase.class);

	private static Configuration conf = null;
	public HTableInterface table = null;
	private static HConnection connection = null;

	public static final String COL_FAMILY = "cf";
	public static final byte[] COL_FAMILY_BYTES = Bytes.toBytes(COL_FAMILY);

	public Hbase(String zkUrl, String tableName) {
		try {
			conf = HBaseConfigBuilder.createConfiguration(zkUrl);
			// table = new HTable(conf, TABLE_RELATION_BYTES);
			connection = HConnectionManager.createConnection(conf);
			table = connection.getTable(tableName);
		} catch (IOException e) {
			LOG.error("get the " + tableName + " table error", e);
			throw new RuntimeException("get the " + tableName + " table error", e);
		}
	}

	/**
	 * 使用多线程存在线程安全问题，会出现 flushCommit 异常，可以使用 synchronized 关键字或者使用单线程
	 * 
	 * @param relations
	 */
	public synchronized void put(final List<String> rowKeys, List<String[]> columns, List<String[]> values) {
		List<Put> puts = new ArrayList<>();
		LOG.info("start put data into hbase, size is " + rowKeys.size());

		try {
			for (int i = 0, len = rowKeys.size(); i < len; i++) {
				puts.add(newPut(rowKeys.get(i), columns.get(i), values.get(i)));
			}
			table.put(puts);
			LOG.info("finish put data into hbase, size is " + rowKeys.size());
			puts = null;
		} catch (IOException e) {
			LOG.error("put data into hbase's relation error, colFamily=" + COL_FAMILY, e);
		}
	}

	/**
	 * 向行键中添加指定列族的多个列和值
	 * 
	 * @param rowKey
	 * @param columns
	 * @param values
	 * @return
	 * @throws IOException
	 */
	public Put newPut(String rowKey, String[] columns, String[] values) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));

		for (int j = 0, len = columns.length; j < len; j++) {
			put.add(COL_FAMILY_BYTES, Bytes.toBytes(columns[j]), Bytes.toBytes(values[j]));
		}

		// 这里的put操作不会立即提交数据到远程服务器，要显式调用flushCommits方法才行
		return put;
	}

	public void close() {
		Closeables.close(table);
	}

}