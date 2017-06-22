package com.surfilter.mass.factory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.surfilter.mass.MassConsts;
import com.surfilter.mass.MassContext;
import com.surfilter.mass.MassRunnable;
import com.surfilter.mass.consumer.Consumer;

/**
 * ConsumerFactory
 * 
 * @author zealot
 *
 */
public class ConsumerFactory implements Consumer {

	private ExecutorService executors;

	public ConsumerFactory(MassContext context) {
		this.executors = Executors
				.newFixedThreadPool(context.getInt(MassConsts.CONSUMER_NUM, MassConsts.DEFAULT_CONSUMER_NUM));
	}

	@Override
	public Consumer execute(MassRunnable runner) {
		if (this.executors != null) {
			this.executors.execute(runner);
		}
		return this;
	}

	@Override
	public void shutdown() {
		if (this.executors != null) {
			this.executors.shutdown();
		}
	}

}
