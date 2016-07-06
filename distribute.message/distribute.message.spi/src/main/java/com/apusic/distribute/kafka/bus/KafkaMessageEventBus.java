package com.apusic.distribute.kafka.bus;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apusic.distribute.message.bus.MessageEventConsumer;
import com.apusic.distribute.message.listener.MessageEventHandler;
import com.apusic.distribute.message.model.MessageEvent;

/**
 * Kafka 事件处理中心实现类
 * 
 * @author zt
 *
 */
public class KafkaMessageEventBus implements MessageEventConsumer {
	private final static Logger LOG = LoggerFactory.getLogger(KafkaMessageEventBus.class);

	private String bootstrapServers;

	public KafkaMessageEventBus(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		LOG.info("create KafkaMessageEventBus.");
	}

	/**
	 * 根据 groupId 获取 Kafka 消费者
	 * 
	 * @param groupId
	 * @return
	 */
	private <T extends Serializable> Consumer<Long, MessageEvent<T>> getKafkaConsumer(String groupId) {
		Properties props = new Properties();

		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "com.apusic.distribute.kafka.serializable.MessageEventDeserializer");

		return new KafkaConsumer<Long, MessageEvent<T>>(props);
	}

	@Override
	public <T extends Serializable> void consumer(String groupId, List<String> eventTypes,
			MessageEventHandler<T> handler) {
		Consumer<Long, MessageEvent<T>> consumer = getKafkaConsumer(groupId);

		// 根据消息类型订阅消息(消息类型充当 kafka 的topic)
		consumer.subscribe(eventTypes);

		while (true) {
			ConsumerRecords<Long, MessageEvent<T>> records = consumer.poll(100);
			for (ConsumerRecord<Long, MessageEvent<T>> record : records) {
				// 调用消息处理接口处理消息
				handler.handler(record.value());
			}
		}
	}

	@Override
	public <T extends Serializable> void consumer(String groupId, String eventType, MessageEventHandler<T> handler) {
		this.consumer(groupId, Arrays.asList(eventType), handler);
	}
}
