package com.surfilter.mass.consumer;

import com.surfilter.mass.MassRunnable;

/**
 * 
 * @author zealot
 *
 */
public interface Consumer {

	/**
	 * submit
	 * 
	 * @param runner
	 * @return
	 */
	Consumer execute(MassRunnable runner);

	/**
	 * shutdown
	 */
	void shutdown();
}
