package com.sky.projects.kafka.common;

/**
 * Kafka 配置信息
 * 
 * @author zt
 *
 */
public final class KafkaConfig {

	public final static String ZK_CONNECT_PROPERTY = "zookeeper.connect";
	public final static String GROUP_ID_PROPERTY = "group.id";
	public final static String ZK_SESSION_TIMEOUT_MS_PROPERTY = "zookeeper.session.timeout.ms";
	public final static String ZK_SYNC_TIME_MS_PROPERTY = "zookeeper.sync.time.ms";
	public final static String AUTO_COMMIT_INTERVAL_MS_PROPERTY = "auto.commit.interval.ms";
	public final static String METADAT_BROKER_LIST_PROPERTY = "metadata.broker.list";
	public final static String SERIALIZER_CLASS_PROPERTY = "serializer.class";
	public final static String AUTO_OFFSET_RESET_PROPERTY = "auto.offset.reset";

	public final static String DEFAULT_OFFSET_RESET = "largest";
	public final static String DEFAULT_METADAT_BROKER_LIST = "localhost:9092";
	public final static String DEFAULT_AUTO_COMMIT_INTERVAL_MS = "1000";
	public final static String DEFAULT_ZK_SYNC_TIME_MS = "200";
	public final static String DEFAULT_ZK_SESSION_TIMEOUT_MS = "40000";
	public final static String DEFAULT_ZK_CONNECT = "localhost:2181";
	public final static String DEFAULT_SERIALIZER_CLASS = "kafka.serializer.StringEncoder";

	private KafkaConfig() {
	}
}