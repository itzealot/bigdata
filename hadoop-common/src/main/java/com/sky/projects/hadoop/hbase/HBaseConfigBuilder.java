package com.sky.projects.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HBaseConfigBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseConfigBuilder.class);

	/**
	 * hbase-site.xml, core-site.xml配置文件放在 CLASSPATH 下，系统会自动加载
	 * 
	 * config.addResource(new Path(System.getenv("HBASE_HOME"),
	 * "conf/hbase-site.xml"));
	 * 
	 * config.addResource(new Path(System.getenv("HADOOP_HOME"),
	 * "etc/hadoop/core-site.xml"));
	 */
	public static Configuration createConfiguration(String zkUrl) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", zkUrl);
		return config;
	}

	public static HBaseAdmin create(String zkUrl) {
		try {
			return new HBaseAdmin(createConfiguration(zkUrl));
		} catch (IOException e) {
			LOG.error("HBaseAdmin create by zkUrl error, zkUrl={}, {}", zkUrl, e);
			throw new RuntimeException("HBaseAdmin create by zkUrl error.");
		}
	}

	public static HTableInterface create(HConnection conn, String tableName) {
		try {
			return conn.getTable(tableName);
		} catch (IOException e) {
			throw new RuntimeException("HTable create by HConnection error.");
		}
	}

	public static HConnection build(Configuration conf) {
		try {
			return HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			throw new RuntimeException("HConnection build by Configuration error.");
		}
	}

	public static HConnection build(String zkUrl) {
		return build(createConfiguration(zkUrl));
	}

	public static HBaseAdmin create(Configuration conf) {
		try {
			return new HBaseAdmin(conf);
		} catch (IOException e) {
			LOG.error("HBaseAdmin create by Configuration error", e);
			throw new RuntimeException("HBaseAdmin create by Configuration error.");
		}
	}

	private HBaseConfigBuilder() {
	}
}
