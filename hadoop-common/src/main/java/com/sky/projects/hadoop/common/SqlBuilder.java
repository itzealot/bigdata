package com.sky.projects.hadoop.common;

import java.util.List;

/**
 * 
 * @author zealot
 */
public final class SqlBuilder {

	public static String count(String tableName) {
		return "SELECT count(*) FROM " + tableName;
	}

	public static String query(String tableName, int limit) {
		return "SELECT * FROM " + tableName + " LIMIT " + limit;
	}

	public static String query(String tableName, int limit, String... fileds) {
		return "SELECT " + join(',', fileds) + " FROM " + tableName + " LIMIT " + limit;
	}

	public static String query(String tableName, int limit, List<String> fileds) {
		return "SELECT " + join(',', fileds) + " FROM " + tableName + " LIMIT " + limit;
	}

	public static String showAll() {
		return "show tables";
	}

	public static String show(String regex) {
		return "show tables '" + regex + "'";
	}

	private static String join(char spliter, String... strings) {
		StringBuffer buffer = new StringBuffer();

		int i = 0;
		for (int len = strings.length; i < len - 1; i++) {
			buffer.append(strings[i]);
			buffer.append(spliter);
		}

		buffer.append(strings[i]);

		return buffer.toString();
	}

	private static String join(char spliter, List<String> fileds) {
		StringBuffer buffer = new StringBuffer();

		int i = 0;
		for (int len = fileds.size(); i < len - 1; i++) {
			buffer.append(fileds.get(i));
			buffer.append(spliter);
		}

		buffer.append(fileds.get(i));

		return buffer.toString();
	}

	private SqlBuilder() {
	}
}
