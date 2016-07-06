package com.apusic.distribute.message.bus;

import com.apusic.distribute.message.listener.MessageEventHandler;

import java.io.Serializable;
import java.util.List;

/**
 * 事件处理中心接口
 * 
 * @author zt
 *
 */
public interface MessageEventConsumer {

	/**
	 * 根据 groupId 与 topic 消费单个消息,并调用消息处理接口进行消息处理
	 * 
	 * @param groupId
	 *            kafka gruopId
	 * @param eventType
	 *            事件类型即 topic
	 * @param handler
	 *            消息处理接口
	 */
	<T extends Serializable> void consumer(String groupId, String eventType, MessageEventHandler<T> handler);

	/**
	 * 根据 groupId 与 topic 消费一组消息,并调用消息处理接口进行消息处理
	 * 
	 * @param groupId
	 *            kafka gruopId
	 * @param eventTypes
	 *            事件类型即 topics
	 * @param handler
	 *            消息处理接口
	 */
	<T extends Serializable> void consumer(String groupId, List<String> eventTypes, MessageEventHandler<T> handler);
}
