package com.sky.projects.registry.util;

import java.util.Date;

import com.sky.projects.registry.entity.RegistryInfo;

public final class RegistryFactory {

	public static RegistryInfo newInstance(String registerName, String version) {
		return new RegistryInfo(registerName, NetUtil.getLocalAddress().getHostAddress(), NetUtil.getLocalHost(),
				new Date().getTime() / 1000, SystemPropertiesUtil.getString("user.dir"), version);
	}

	private RegistryFactory() {
	}
}
