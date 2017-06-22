package com.surfilter.mass.util;

import static java.lang.String.format;

/**
 * 
 * @author zealot
 *
 */
public final class ReflectUtil {

	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String className) {
		try {
			return (T) Class.forName(className).newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException(format("new instance fail, className:%s", className), e);
		}
	}

	private ReflectUtil() {
	}
}
