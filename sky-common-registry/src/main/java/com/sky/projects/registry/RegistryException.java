package com.sky.projects.registry;

public class RegistryException extends RuntimeException {

	private static final long serialVersionUID = 8212987127982646304L;

	public RegistryException() {
		super();
	}

	public RegistryException(String message, Throwable cause) {
		super(message, cause);
	}

	public RegistryException(String message) {
		super(message);
	}

}
