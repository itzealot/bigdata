package com.sky.projects.hadoop.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkyConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(SkyConfiguration.class);

	private Configuration config;
	private static final String CONFIG_DIR_NAME = "/conf/";
	private static final String CONFIG_FILE_NAME = "conf.properties";

	public SkyConfiguration() {
		try {
			// for linux
			config = new PropertiesConfiguration(System.getProperty("user.dir") + CONFIG_DIR_NAME + CONFIG_FILE_NAME);

			// for eclipse
			// config = new PropertiesConfiguration(CONFIG_FILE_NAME);
		} catch (ConfigurationException e) {
			LOG.error("read configure error, {}", e);
			new RuntimeException("read configure error");
		}
	}

	public String get(String key) {
		return this.config.getString(key);
	}

	public Integer getInt(String key) {
		return this.config.getInt(key);
	}

	public Integer getInt(String key, int defaultValue) {
		return this.config.getInt(key, defaultValue);
	}

	public String[] getArray(String key) {
		return this.config.getStringArray(key);
	}

	public String getStrings(String key) {
		String[] results = getArray(key);
		if (results == null || results.length == 0)
			return null;

		StringBuilder stringBuilder = new StringBuilder();

		if (results != null && results.length > 0) {
			for (int i = 0; i < results.length; i++) {
				if (i == results.length - 1) {
					stringBuilder.append(results[i]);
				} else {
					stringBuilder.append(results[i]).append(",");
				}
			}
		}

		return stringBuilder.toString();
	}
}