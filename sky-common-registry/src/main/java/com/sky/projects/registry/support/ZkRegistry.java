package com.sky.projects.registry.support;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.registry.Registry;
import com.sky.projects.registry.RegistryException;
import com.sky.projects.registry.entity.RegistryInfo;

/**
 * ZkRegistry
 * 
 * @author zealot
 *
 */
public class ZkRegistry extends ZkRegistryClient implements Registry<RegistryInfo> {

	private static Logger LOG = LoggerFactory.getLogger(ZkRegistry.class);

	public ZkRegistry(String url) {
		super(url);
	}

	/**
	 * 
	 * @param url
	 * @param rootPath
	 * @throws RegistryException
	 *             when start zookeeper client fail
	 */
	public ZkRegistry(String url, String rootPath) {
		super(url, rootPath);
	}

	@Override
	public void register(RegistryInfo info) {
		createNodeRecurse(info);

		// 注册监听器
		newClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
				// 节点状态变更，当失去连接时，重新创建节点
				if (connectionState == ConnectionState.LOST) {
					createNodeRecurse(info);
				}
			}
		});
	}

	private void createNodeRecurse(RegistryInfo info) {
		if (this.newClient != null) {
			try {
				ServiceInstanceBuilder<RegistryInfo> sib = ServiceInstance.builder();
				ServiceInstance<RegistryInfo> service = sib.name(info.getRegisterName()).payload(info).build();
				serviceDiscovery.registerService(service);// 服务注册
				serviceDiscovery.start();
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("regist service instance fail.", e);
			}
		}
	}

	@Override
	public void close() {
		if (serviceDiscovery != null) {
			try {
				serviceDiscovery.close();
			} catch (IOException e) {
			}
		}

		if (newClient != null) {
			newClient.close();
		}
	}
}
