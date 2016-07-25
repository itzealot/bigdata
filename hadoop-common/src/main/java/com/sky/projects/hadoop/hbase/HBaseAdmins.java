package com.sky.projects.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

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
public final class HBaseAdmins {

	/**
	 * 创建新表
	 * 
	 * Creates a table if not exist. Synchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean creatTableIfNotExist(HBaseAdmin admin, String tableName, String[] columnFamily) {
		return exists(admin, tableName) ? false : createNewTable(admin, tableName, columnFamily);
	}

	/**
	 * 根据给定的多个列族创建新表
	 * 
	 * Creates a new table. Synchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean createNewTable(HBaseAdmin admin, String tableName, String[] columnFamily) {
		try {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
			for (int i = 0, len = columnFamily.length; i < len; i++) {
				table.addFamily(new HColumnDescriptor(columnFamily[i])); // 添加列族
			}
			admin.createTable(table);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * 表是否存在，网络异常时，默认不存在.
	 * 
	 * @param tableName
	 * @return
	 */
	public static boolean exists(HBaseAdmin admin, String tableName) {
		try {
			return admin.tableExists(tableName);
		} catch (IOException e) {
			return false;
		}
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
	public static void addColumns(HBaseAdmin admin, String tableName, List<String> columnFamilies) {
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
	 * 添加列族
	 * 
	 * Add a column to an existing table. Asynchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamily
	 */
	public static void addColumn(HBaseAdmin admin, String tableName, String columnFamily) {
		addColumns(admin, tableName, Arrays.asList(columnFamily));
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
	public static void modifyColumns(HBaseAdmin admin, String tableName, List<String> columnFamilies) {
		try {
			for (String columnFamily : columnFamilies) {
				admin.modifyColumn(tableName, new HColumnDescriptor(columnFamily));
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}
	}

	public static List<String> listTables(HBaseAdmin admin) {
		List<String> tables = new ArrayList<>();

		try {
			TableName[] names = admin.listTableNames();
			if (names != null)
				for (TableName name : names) {
					tables.add(Bytes.toString(name.getName()));
				}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
		}

		return tables;
	}

	/**
	 * 修改列族
	 * 
	 * Modify an existing column family on a table. Asynchronous operation.
	 * 
	 * @param tableName
	 * @param columnFamilies
	 */
	public static void modifyColumn(HBaseAdmin admin, String tableName, String columnFamily) {
		modifyColumns(admin, tableName, Arrays.asList(columnFamily));
	}

	public static void deleteTable(HBaseAdmin admin, String tableName) {
		deleteTables(admin, Arrays.asList(tableName));
	}

	/**
	 * 根据表名称批量删除表
	 * 
	 * @param tableNames
	 */
	public static void deleteTables(HBaseAdmin admin, List<String> tableNames) {
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
	public static void deleteTables(HBaseAdmin admin, String regex) {
		try {
			admin.disableTables(regex);
			admin.deleteTables(regex);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}

	private HBaseAdmins() {
	}
}
