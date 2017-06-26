package com.sky.projects.registry.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.projects.registry.Discovery;
import com.sky.projects.registry.entity.RegistryInfo;

public class ZkRegistryDiscovery extends ZkRegistryClient implements Discovery<RegistryInfo> {
	private static Logger LOG = LoggerFactory.getLogger(ZkRegistryDiscovery.class);

	public ZkRegistryDiscovery(String zkUrl) {
		super(zkUrl);
	}

	public ZkRegistryDiscovery(String zkUrl, String rootPath) {
		super(zkUrl, rootPath);
	}

	@Override
	public Collection<String> listRegistryNames() {
		if (serviceDiscovery == null) {
			return null;
		}

		try {
			return serviceDiscovery.queryForNames();
		} catch (Exception e) {
			LOG.error("query services' name fail.", e);
			return null;
		}
	}

	@Override
	public List<RegistryInfo> listRegistry(String name) {
		List<RegistryInfo> registryInfos = null;

		try {
			if (serviceDiscovery != null) {
				Collection<ServiceInstance<RegistryInfo>> services = serviceDiscovery.queryForInstances(name);
				if (services != null && services.size() > 0) {
					registryInfos = new ArrayList<>(services.size());

					for (ServiceInstance<RegistryInfo> service : services) {
						registryInfos.add(service.getPayload());
					}
				}
			}
		} catch (Exception e) {
			LOG.error(String.format("query registry infos fail, rootPath:%s, name:%s", rootPath, name), e);
		}
		return registryInfos;
	}
}
