package com.surfilter.mass.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import com.surfilter.mass.MassConsts;
import com.surfilter.mass.MassContext;
import com.surfilter.mass.MassRunnable;

/**
 * KafkaStringConsumer
 * 
 * @author zealot
 *
 */
public abstract class KafkaStringConsumerRunner implements MassRunnable {

	private volatile boolean running = true;
	private final BlockingQueue<String> queue;
	protected final MassContext context;
	private final int batchSize;

	public KafkaStringConsumerRunner(MassContext context) {
		this.context = context;
		this.queue = context.getStringQueue();
		this.batchSize = context.getInt(MassConsts.CONSUMER_CONSUME_BATCH_SIZE,
				MassConsts.DEFAULT_CONSUMER_CONSUME_BATCH_SIZE);
	}

	@Override
	public final void run() {
		while (running || !queue.isEmpty()) {
			Collection<String> messages = new ArrayList<>(batchSize);

			int size = queue.drainTo(messages);

			if (size > 0) {
				this.doRun(messages);
				messages.clear();
			}
		}
	}

	public abstract void doRun(Collection<String> messages);

	@Override
	public final void shutdown() {
		this.running = false;
	}

}
