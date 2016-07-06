package com.apusic.distribute.message.publisher;

import com.apusic.distribute.message.model.MessageEvent;

import java.io.Serializable;

/**
 * 发送消息事件实体(含消息事件类型，消息，发送时间) 接口
 * 
 * @author zt
 *
 * @param <T>
 */
public interface MessageEventPublisher<T extends Serializable> {

	/**
	 * 接收并发送消息事件实体(含消息事件类型，消息，发送时间)
	 * 
	 * topic : eventType
	 * 
	 * key : sentTime
	 * 
	 * value : MessageEvent<T>
	 * 
	 * @param event
	 *            要发送的消息
	 */
	void publish(MessageEvent<T> event);

}
