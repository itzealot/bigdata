package com.sky.projects.hadoop.hbase;

import static com.sky.projects.hadoop.common.Closeables.close;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.hadoop.hive.Hives;

/**
 * HBase 基本操作
 * 
 * HBaseAdmin ---> Admin
 * 
 * HTableDescriptor ---> Table
 * 
 * HColumnDescriptor ---> ColumnFamily
 * 
 * @author zt
 */
public final class Hbases {
	private static final Logger LOG = LoggerFactory.getLogger(Hives.class);

	private static final String zkUrl = "rzx162:2181,rzx164:2181,rzx166:218";
	private static Configuration conf = HBaseConfigBuilder.createConfiguration(zkUrl);
	private static HBaseAdmin admin = HBaseConfigBuilder.create(conf);
	public static HTableInterface table = null;
	private static HConnection connection = null;

	static {
		try {
			connection = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			LOG.error("create Hbase Connection error.", e);
		}
	}

	public static void init() {
		// 初始化 Hbases，如初始化 Configuration
	}

	/**
	 * 根据rowKey范围遍历查询 hbase 表
	 * 
	 * @param tableName
	 * @param startRowkey
	 * @param stopRowkey
	 * @throws IOException
	 */
	public static List<HbaseCell> getResultScann(String tableName, String startRowkey, String stopRowkey) {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRowkey));
		scan.setStopRow(Bytes.toBytes(stopRowkey));
		List<HbaseCell> results = new ArrayList<>();
		ResultScanner rs = null;
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			rs = table.getScanner(scan);

			for (Result r : rs) {
				for (Cell cell : r.listCells()) {
					String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
					String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					long timestamp = cell.getTimestamp();
					results.add(new HbaseCell(rowKey, columnFamily, qualifier, value, timestamp));
				}
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			close(rs, table);
		}

		return results;
	}

	/**
	 * 根据rokKey与columnFamily 查询Hbase表中的某一列的值并返回
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 * @return
	 * @throws IOException
	 */
	public static HbaseCell query(String tableName, String rowKey, String columnFamily, String columnName) {
		Get get = new Get(Bytes.toBytes(rowKey));
		HTable table = null;
		Result result = null;
		HbaseCell hbaseResult = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

			result = table.get(get);
			for (Cell cell : result.listCells()) {
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				long timestamp = cell.getTimestamp();
				hbaseResult = new HbaseCell(rowKey, columnFamily, qualifier, value, timestamp);
				break;
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			close(table);
		}

		return hbaseResult;
	}

	/**
	 * 根据rokKey 查询Hbase表中的某一列族并返回
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 * @return
	 * @throws IOException
	 */
	public static List<HbaseCell> query(String tableName, String rowKey, String columnFamily) {
		Get get = new Get(Bytes.toBytes(rowKey));
		List<HbaseCell> results = new ArrayList<>();
		HTable table = null;
		Result result = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			// 获取指定列族和列修饰符对应的列
			get.addFamily(Bytes.toBytes(columnFamily));
			result = table.get(get);
			for (Cell cell : result.listCells()) {
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				long timestamp = cell.getTimestamp();
				results.add(new HbaseCell(rowKey, columnFamily, qualifier, value, timestamp));
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			close(table);
		}

		return results;
	}

	public static HbaseCell get(String tableName, String rowKey, String columnFamily, String columnName,
			int maxVersions) {
		HTable table = null;
		Result result = null;
		HbaseCell hbaseResult = null;

		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			table = new HTable(conf, Bytes.toBytes(tableName));
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
			get.setMaxVersions(maxVersions);

			result = table.get(get);
			for (Cell cell : result.listCells()) {
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				long timestamp = cell.getTimestamp();
				hbaseResult = new HbaseCell(rowKey, columnFamily, qualifier, value, timestamp);
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			close(table);
		}

		return hbaseResult;
	}

	public static long count(String tableName) {
		try {
			return new AggregationClient(conf).rowCount(TableName.valueOf(tableName), new DoubleColumnInterpreter(),
					new Scan());
		} catch (Throwable e) {
			// TODO
			e.printStackTrace();
			return 0;
		}

	}

	public static long count(String tableName, String family) {
		try {
			return new AggregationClient(conf).rowCount(TableName.valueOf(tableName), new DoubleColumnInterpreter(),
					new Scan().addFamily(Bytes.toBytes(family)));
		} catch (Throwable e) {
			// TODO
			e.printStackTrace();
			return 0;
		}
	}

	private Hbases() {
	}
}