package com.sky.projects.registry;

import java.util.Collection;
import java.util.List;

public interface Discovery<T> {

	/**
	 * 返回当前路径下的所有已注册节点名称
	 * 
	 * @return
	 */
	Collection<String> listRegistryNames();

	/**
	 * 在当前路径下根据节点名称返回所有的已注册信息实体
	 * 
	 * @param name
	 * @return
	 */
	List<T> listRegistry(String name);

}
