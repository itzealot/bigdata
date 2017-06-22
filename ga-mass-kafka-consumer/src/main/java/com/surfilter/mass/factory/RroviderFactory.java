package com.surfilter.mass.factory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.surfilter.mass.provider.Provider;

/**
 * RroviderFactory
 * 
 * @author zealot
 *
 */
public class RroviderFactory implements Provider {

	private Map<String, Provider> providers;

	public RroviderFactory() {
		this.providers = new ConcurrentHashMap<>();
	}

	public RroviderFactory(int capacity) {
		this.providers = new ConcurrentHashMap<>(capacity);
	}

	public RroviderFactory register(String key, Provider provider) {
		if (provider != null && key != null) {
			providers.put(key, provider);
		}
		return this;
	}

	@Override
	public void close() {
		if (providers != null) {
			for (Map.Entry<String, Provider> entry : providers.entrySet()) {
				entry.getValue().close();
			}
		}
	}

	@Override
	public void provide() {
		for (Map.Entry<String, Provider> entry : providers.entrySet()) {
			entry.getValue().provide();
		}
	}

}
