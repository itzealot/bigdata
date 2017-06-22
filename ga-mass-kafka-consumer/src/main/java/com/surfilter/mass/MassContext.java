package com.surfilter.mass;

import java.util.concurrent.BlockingQueue;

/**
 * MassContext
 * 
 * @author zealot
 *
 */
public interface MassContext {

	BlockingQueue<String> getStringQueue();

	/**
	 * get String split by ,
	 * 
	 * @param key
	 * @return
	 */
	String getStrings(String key);

	String get(String key);

	String get(String key, String defaultValue);

	Integer getInt(String key);

	Integer getInt(String key, int defaultValue);

	Long getLong(String key);

	Long getLong(String key, long defaultValue);

}
