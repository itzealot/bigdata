package com.sky.projects.registry.support;

import com.sky.projects.registry.util.RegistryFactory;

public class ZkRegistryTest {

	public static void main(String[] args) {
		String zkUrl = "loclhost:2181";
		ZkRegistry registry = new ZkRegistry(zkUrl);

		Thread thread = new Thread(() -> {
			registry.register(RegistryFactory.newInstance("tools-test", "V0.0.1"));
		});

		thread.setDaemon(true);
		thread.start();

		try {
			while (true) {
				Thread.sleep(5000);
			}
		} catch (InterruptedException e) {
		} finally {
			registry.close();
		}
	}

}
