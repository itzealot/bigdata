package com.surfilter.mass.provider;

/**
 * Provider
 * 
 * @author zealot
 *
 */
public interface Provider extends AutoCloseable {

	/**
	 * provide data
	 */
	void provide();

	/**
	 * close
	 */
	void close();

}
