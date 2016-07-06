package com.sky.projects.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public final class HBaseConfigBuilder {

	public static Configuration createConfiguration(String zkUrl) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", zkUrl);
		// hbase-site.xml, core-site.xml配置文件放在 CLASSPATH 下，系统会自动加载
		config.addResource(new Path(System.getenv("HBASE_HOME"), "conf/hbase-site.xml"));
		config.addResource(new Path(System.getenv("HADOOP_HOME"), "etc/hadoop/core-site.xml"));

		return config;
	}

	public static HBaseAdmin create(String zkUrl) {
		try {
			return new HBaseAdmin(createConfiguration(zkUrl));
		} catch (IOException e) {
			throw new RuntimeException("HBaseAdmin create by zkUrl error, zkUrl=" + zkUrl, e);
		}
	}

	public static HBaseAdmin create(Configuration conf) {
		try {
			return new HBaseAdmin(conf);
		} catch (IOException e) {
			throw new RuntimeException("HBaseAdmin create by Configuration error.", e);
		}
	}

	private HBaseConfigBuilder() {
	}
}
