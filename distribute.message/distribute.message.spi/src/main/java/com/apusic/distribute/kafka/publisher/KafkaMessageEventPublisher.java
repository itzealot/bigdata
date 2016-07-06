package com.apusic.distribute.kafka.publisher;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.apusic.distribute.message.model.MessageEvent;
import com.apusic.distribute.message.publisher.MessageEventPublisher;

/**
 * Kafka 发送消息事件实体(含消息事件类型，消息，发送时间)
 * 
 * @author zt
 *
 */
public class KafkaMessageEventPublisher<T extends Serializable> implements MessageEventPublisher<T> {
	// 生产者
	private Producer<Long, MessageEvent<T>> producer;
	private String bootstrapServers;

	public KafkaMessageEventPublisher(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		producer = getKafkaProducer();
	}

	/**
	 * 获取 Kafka 生产者
	 * 
	 * @return
	 */
	private Producer<Long, MessageEvent<T>> getKafkaProducer() {
		Properties props = new Properties();

		props.put("bootstrap.servers", bootstrapServers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "com.apusic.distribute.kafka.serializable.MessageEventSerializer");

		return new KafkaProducer<Long, MessageEvent<T>>(props);
	}

	@Override
	public void publish(MessageEvent<T> messageEvent) {
		/*
		 * ProducerRecord(String topic, K key, V value)
		 * 
		 * topic : eventType
		 * 
		 * key : sentTime
		 * 
		 * value : MessageEvent<T>
		 */
		producer.send(new ProducerRecord<Long, MessageEvent<T>>(messageEvent.getEventType(),
				messageEvent.getTime().getTime(), messageEvent));

		producer.flush();
		producer.close();
	}

}
