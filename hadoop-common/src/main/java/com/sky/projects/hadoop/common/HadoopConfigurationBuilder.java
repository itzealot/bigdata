package com.sky.projects.hadoop.common;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @author zealot
 */
public final class HadoopConfigurationBuilder {

	public static Configuration create(String hdfs) {
		Configuration conf = new Configuration();

		conf.set("fs.default.name", hdfs);

		return conf;
	}

	private HadoopConfigurationBuilder() {
	}
}
