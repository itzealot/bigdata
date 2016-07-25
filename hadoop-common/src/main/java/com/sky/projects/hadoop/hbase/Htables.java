package com.sky.projects.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.sky.projects.hadoop.common.Closeables;

public final class Htables {

	/**
	 * 删除 HTable 中的单行
	 * 
	 * @param tableName
	 * @param rowKeys
	 */
	public static void deleteRowBy(HTable table, String rowKey) {
		deleteRowBy(table, Arrays.asList(rowKey));
	}

	/**
	 * 删除 HTable 中的多行
	 * 
	 * @param tableName
	 * @param rowKeys
	 */
	public static void deleteRowBy(HTable table, List<String> rowKeys) {
		try {
			for (String rowKey : rowKeys) {
				table.delete(new Delete(Bytes.toBytes(rowKey)));
			}
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * Puts some data in the table.If isAutoFlush is false, the update is
	 * buffered until the internal buffer is full.
	 * 
	 * 给定行健与列族添加多个列与对应的值
	 * 
	 * @param table
	 * @param rowKey
	 * @param columnFamily
	 * @param columns
	 * @param values
	 */
	public static void put(HTable table, String rowKey, String columnFamily, String[] columns, String[] values) {
		try {
			table.put(newPut(rowKey, columnFamily, columns, values));
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * Puts some data in the table, in batch.
	 * 
	 * @param table
	 * @param rowKeys
	 * @param columnFamily
	 * @param columnLists
	 * @param valueLists
	 */
	public static void put(HTable table, List<String> rowKeys, String columnFamily, List<String[]> columnLists,
			List<String[]> valueLists) {
		try {
			List<Put> puts = new ArrayList<>();
			// hbase 1.0.1之后的版本不提倡用此函数，提倡用BufferedMutatorParams函数
			// table.setWriteBufferSize(12 * 1024 * 1024);
			// table.setAutoFlushTo(false);

			for (int i = 0, len = rowKeys.size(); i < len; i++) {
				puts.add(newPut(rowKeys.get(i), columnFamily, columnLists.get(i), valueLists.get(i)));
			}

			table.put(puts);
			// table.flushCommits();
			puts = null;
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
	}

	public static Put newPut(String rowKey, String columnFamily, String[] columns, String[] values) {
		Put put = new Put(Bytes.toBytes(rowKey));
		byte[] family = Bytes.toBytes(columnFamily);

		for (int i = 0, len = columns.length; i < len; i++) {
			put.add(family, Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
		}

		return put;
	}

	/**
	 * 根据rowKey, columnFamily, columnName 删除一个单元
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 */
	public static void deleteColumn(HTable table, String rowKey, String columnFamily, String columnName) {
		try {
			table.delete(new Delete(Bytes.toBytes(rowKey)).deleteColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(columnName)));
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * 根据rowKey, columnFamily, columnName, timestamp 删除一个单元
	 * 
	 * Deletes the specified cells/row.
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 */
	public static void deleteColumn(HTable table, String rowKey, String columnFamily, String columnName,
			long timestamp) {
		try {
			table.delete(new Delete(Bytes.toBytes(rowKey)).deleteColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(columnName), timestamp));
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}

	/**
	 * 根据 rwokey 查询，返回相应的值
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public static <T> T get(HTable table, String rowKey, ResultCallBack<T> call) {
		try {
			return call.call(table.get(new Get(Bytes.toBytes(rowKey))));
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 遍历查询 hbase 表
	 * 
	 * @param tableName
	 * @param limit
	 * @return
	 */
	public static List<HbaseCell> getResultScann(HTable table, int limit) {
		Scan scan = new Scan();
		ResultScanner rs = null;
		List<HbaseCell> results = new ArrayList<>();
		int counts = 0;

		try {
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
			Closeables.close(rs);
		}

		return results;
	}

	public static void close(HTable table) {
		Closeables.close(table);
	}

	public static interface CellCallBack<T> {
		public T call(Cell cell);
	}

	public static interface ResultCallBack<T> {
		public T call(Result result);
	}

	public static interface CellsCallBack<T> {
		public T call(List<Cell> cells);
	}

	private Htables() {
	}
}
