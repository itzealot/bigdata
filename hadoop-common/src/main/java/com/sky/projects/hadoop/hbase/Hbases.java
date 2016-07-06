package com.sky.projects.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

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
public final class Hbases {
	private static String zkUrl = "localhost";
	private static Configuration conf = HBaseConfigBuilder.createConfiguration(zkUrl);
	private static HBaseAdmin admin = HBaseConfigBuilder.create(zkUrl);

	public static void init() {
		// 初始化 Hbases，如初始化 Configuration
	}

	/**
	 * 创建新表
	 * 
	 * Creates a table if not exist. Synchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean creatTable(String tableName, String[] columnFamily) {
		return exists(tableName) ? false : createNewTable(tableName, columnFamily);
	}

	/**
	 * 创建新表
	 * 
	 * Creates a new table. Synchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean createNewTable(String tableName, String[] columnFamily) {
		try {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
			for (int i = 0, len = columnFamily.length; i < len; i++) {
				table.addFamily(new HColumnDescriptor(columnFamily[i])); // 添加列族
			}
			admin.createTable(table);
			return true;
		} catch (IOException e) {
			// TODO
			return false;
		}
	}

	/**
	 * 表是否存在，网络异常时，默认不存在.
	 * 
	 * @param tableName
	 * @return
	 */
	public static boolean exists(String tableName) {
		try {
			return admin.tableExists(tableName);
		} catch (IOException e) {
			// TODO
			return false;
		}
	}

	/**
	 * 添加列族
	 * 
	 * Add a column to an existing table. Asynchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamily
	 */
	public static void addColumn(String tableName, String columnFamily) {
		addColumns(tableName, Arrays.asList(columnFamily));
	}

	/**
	 * 添加列族
	 * 
	 * Add columns to an existing table. Asynchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamilies
	 *            column descriptor of column to be added
	 */
	public static void addColumns(String tableName, List<String> columnFamilies) {
		try {
			for (String columnFamily : columnFamilies) {
				admin.addColumn(tableName, new HColumnDescriptor(columnFamily));
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * 修改列族
	 * 
	 * Modify existing columns family on a table. Asynchronous operation.
	 * 
	 * @param tableName
	 *            name of table
	 * @param columnFamilies
	 *            new columns descriptor to use
	 */
	public static void modifyColumns(String tableName, List<String> columnFamilies) {
		try {
			for (String columnFamily : columnFamilies) {
				admin.modifyColumn(tableName, new HColumnDescriptor(columnFamily));
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * 修改列族
	 * 
	 * Modify an existing column family on a table. Asynchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamilies
	 */
	public static void modifyColumn(String tableName, String columnFamily) {
		modifyColumns(tableName, Arrays.asList(columnFamily));
	}

	public static List<String> listTables() {
		HBaseAdmin admin = null;
		List<String> tables = new ArrayList<>();

		try {
			admin = new HBaseAdmin(conf);
			TableName[] names = admin.listTableNames();
			if (names != null)
				for (TableName name : names) {
					tables.add(Bytes.toString(name.getName()));
				}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(admin);
		}

		return tables;
	}

	public static void put(String rowKey, String tableName, String columnFamily, String[] column, String[] value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			byte[] family = Bytes.toBytes(columnFamily);

			for (int i = 0, len = column.length; i < len; i++) {
				put.add(family, Bytes.toBytes(column[i]), Bytes.toBytes(value[i]));
			}

			table.put(put);
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}
	}

	/**
	 * 为表添加数据，列族不存在则创建
	 * 
	 * @param rowKey
	 * @param tableName
	 * @param column
	 * @param value
	 */
	public static void put(String rowKey, String tableName, String columnFamily, String[] column, String[] value,
			boolean create) {
		Put put = new Put(Bytes.toBytes(rowKey));
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			int lenJ = column.length;
			byte[] family = Bytes.toBytes(columnFamily);
			for (int j = 0; j < lenJ; j++) {
				put.add(family, Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
			}
			table.put(put);
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}
	}

	public void putList(List<String> rowKeys, String tableName, String colFamily, String[] column, String[] value) {
		List<Put> puts = new ArrayList<>();
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));

			for (String rowKey : rowKeys) {
				Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey

				// hbase 1.0.1之后的版本不提倡用此函数，提倡用BufferedMutatorParams函数
				table.setAutoFlushTo(false);
				table.setWriteBufferSize(12 * 1024 * 1024);

				int lenJ = column.length;
				for (int j = 0; j < lenJ; j++) {
					put.add(Bytes.toBytes(colFamily), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
				}
			}

			table.put(puts);
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}
	}

	/**
	 * 根据 rwokey 查询
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public static Map<String, String> get(String tableName, String rowKey) {
		Map<String, String> map = new HashMap<>();
		HTable table = null;
		Result result = null;
		Get get = new Get(Bytes.toBytes(rowKey));

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			result = table.get(get);
			for (Cell cell : result.listCells()) {
				String value = Bytes.toString(CellUtil.cloneQualifier(cell)) + "|"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "|" + cell.getTimestamp();
				map.put(Bytes.toString(CellUtil.cloneFamily(cell)), value);
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}

		return map;
	}

	/**
	 * 遍历查询 hbase 表
	 * 
	 * @param tableName
	 * @param limit
	 * @return
	 */
	public static List<HbaseCell> getResultScann(String tableName, int limit) {
		Scan scan = new Scan();
		ResultScanner rs = null;
		List<HbaseCell> results = new ArrayList<>();
		HTable table = null;
		int counts = 0;

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

					if (++counts >= limit) {
						break;
					}
				}
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(rs, table);
		}

		return results;
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
			Closeables.close(rs, table);
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
			Closeables.close(table);
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
			Closeables.close(table);
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
			Closeables.close(table);
		}

		return hbaseResult;
	}

	/**
	 * 删除rowKey 中某一列族columnFamily 下的列columnName
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 */
	public static void deleteColumn(String tableName, String rowKey, String columnFamily, String columnName) {
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
			deleteColumn.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
			table.delete(deleteColumn);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}
	}

	/**
	 * 删除rowKey 中某一列族columnFamily 下的列columnName
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 */
	public static void deleteColumn(String tableName, String rowKey, String columnFamily, String columnName,
			long timestamp) {
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
			deleteColumn.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), timestamp);
			table.delete(deleteColumn);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}
	}

	public static void deleteByKey(String tableName, String rowKey) {
		deleteByKey(tableName, Arrays.asList(rowKey));
	}

	/**
	 * 删除表tableName 中的多个rowKey
	 * 
	 * @param tableName
	 * @param rowKeys
	 */
	public static void deleteByKey(String tableName, List<String> rowKeys) {
		HTable table = null;

		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
			for (String rowKey : rowKeys) {
				table.delete(new Delete(Bytes.toBytes(rowKey)));
			}
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		} finally {
			Closeables.close(table);
		}
	}

	public static void deleteTable(String tableName) {
		deleteTables(Arrays.asList(tableName));
	}

	/**
	 * 根据表名称批量删除表
	 * 
	 * @param tableNames
	 */
	public static void deleteTables(List<String> tableNames) {

		try {
			for (String tableName : tableNames) {
				if (admin.tableExists(tableName)) {
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
				}
			}
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * 根据正则表达式删除表，如"raj.*"--> 删除raj开头的表名(*通配多个字符，.通配一个字符)
	 * 
	 * @param regex
	 */
	public static void deleteTables(String regex) {
		try {
			admin.disableTables(regex);
			admin.deleteTables(regex);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
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