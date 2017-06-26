package com.sky.projects.registry;

public interface Registry<T> {

	void register(T t);

	void close();
}
