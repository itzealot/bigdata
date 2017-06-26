package com.sky.projects.registry.support;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import com.sky.projects.registry.RegistryConst;
import com.sky.projects.registry.RegistryException;
import com.sky.projects.registry.entity.RegistryInfo;

public class ZkRegistryClient {

	protected ServiceDiscovery<RegistryInfo> serviceDiscovery;
	protected CuratorFramework newClient;
	protected String rootPath;

	protected ZkRegistryClient(String url) {
		this(url, RegistryConst.REGISTRY_ROOT_PATH);
	}

	/**
	 * new ZkRegistryClient
	 * 
	 * @param zkUrl
	 * @param rootPath
	 * @throws RegistryException
	 *             when start zookeeper client fail
	 */
	protected ZkRegistryClient(String zkUrl, String rootPath) {
		this.rootPath = rootPath;

		try {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			newClient = CuratorFrameworkFactory.newClient(zkUrl, retryPolicy);
			this.initClient(newClient, rootPath);
		} catch (InterruptedException e) {
			throw new RegistryException(String.format("start zookeeper client fail, url:%s", zkUrl), e);
		}
	}

	private void initClient(CuratorFramework newClient, String rootPath) throws InterruptedException {
		if (newClient != null) {
			newClient.start();
			newClient.blockUntilConnected();
			serviceDiscovery = ServiceDiscoveryBuilder.builder(RegistryInfo.class).client(newClient)
					.serializer(new JsonInstanceSerializer<>(RegistryInfo.class)).basePath(rootPath).build();
		}
	}
}
