package com.surfilter.mass;

/**
 * 
 * @author zealot
 *
 */
public final class MassConsts {

	/** kafka 配置信息 */
	public static final String KAFKA_ZK_URL = "kafka.zk.url";
	public static final String KAFKA_GROUP_ID = "kafka.group.id";
	public static final String KAFKA_TOPIC_NAME = "kafka.topic";
	public static final String KAFKA_TOPIC_PARTITION = "kafka.topic.partition";

	/** settings for consumer */
	public static final String CONSUMER_CONSUME_BATCH_SIZE = "consumer.consume.batchSize";
	public static final int DEFAULT_CONSUMER_CONSUME_BATCH_SIZE = 1000;
	public static final String CONSUMER_NUM = "consumer.num";
	public static final int DEFAULT_CONSUMER_NUM = 5;

	private MassConsts() {
	}
}
