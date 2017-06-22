package com.surfilter.mass.util;

/**
 * Number util
 * 
 * @author zealot
 *
 */
public final class NumberUtil {

	public static int parseValue(String item, int defaultValue) {
		try {
			return Integer.parseInt(item);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	public static int parseInt(String item) {
		return parseValue(item, 0);
	}

	public static long parseValue(String item, long defaultValue) {
		try {
			return Long.parseLong(item);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	public static long parseLong(String item) {
		return parseValue(item, 0L);
	}

	public static short parseValue(String item, short defaultValue) {
		try {
			return Short.parseShort(item);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	public static short parseShort(String item) {
		return parseValue(item, (short) 0);
	}

	public static double parseValue(String item, double defaultValue) {
		try {
			return Double.parseDouble(item);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	public static double parseDouble(String item) {
		return parseValue(item, 0.0D);
	}

	public static float parseValue(String item, float defaultValue) {
		try {
			return Float.parseFloat(item);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	public static float parseFloat(String item) {
		return parseValue(item, 0.0F);
	}

	private NumberUtil() {
	}
}
