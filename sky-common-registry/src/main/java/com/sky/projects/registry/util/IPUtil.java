package com.sky.projects.registry.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;

public final class IPUtil {

	/**
	 * 从InetAddress转化到int, 传输和存储时, 用int代表InetAddress是最小的开销.
	 * 
	 * InetAddress可以是IPV4或IPV6，都会转成IPV4.
	 * 
	 * @see com.google.common.net.InetAddresses#coerceToInteger(InetAddress)
	 * 
	 * @param address
	 * @return
	 */
	public static int toInt(InetAddress address) {
		return InetAddresses.coerceToInteger(address);
	}

	/**
	 * InetAddress转换为String.
	 * 
	 * InetAddress可以是IPV4或IPV6. 其中IPV4直接调用getHostAddress()
	 * 
	 * @see com.google.common.net.InetAddresses#toAddrString(InetAddress)
	 * 
	 * @param address
	 * @return
	 */
	public static String toString(InetAddress address) {
		return InetAddresses.toAddrString(address);
	}

	/**
	 * 从int转换为Inet4Address(仅支持IPV4)
	 * 
	 * @param address
	 * @return
	 */
	public static Inet4Address fromInt(int address) {
		return InetAddresses.fromInteger(address);
	}

	/**
	 * 从String转换为InetAddress.
	 * 
	 * IpString可以是ipv4 或 ipv6 string, 但不可以是域名.
	 * 
	 * 先字符串传换为byte[]再调getByAddress(byte[])，避免了调用getByName(ip)可能引起的DNS访问.
	 * 
	 * @param address
	 * @return
	 */
	public static InetAddress fromIpString(String address) {
		return InetAddresses.forString(address);
	}

	/**
	 * 从IPv4String转换为InetAddress.
	 * 
	 * IpString如果确定ipv4, 使用本方法减少字符分析消耗 .
	 * 
	 * 先字符串传换为byte[]再调getByAddress(byte[])，避免了调用getByName(ip)可能引起的DNS访问.
	 * 
	 * @param address
	 * @return
	 * @throws AssertionError
	 */
	public static Inet4Address fromIpv4String(String address) throws AssertionError {
		byte[] bytes = ip4StringToBytes(address);
		if (bytes == null) {
			return null;
		} else {
			try {
				return (Inet4Address) Inet4Address.getByAddress(bytes);
			} catch (UnknownHostException e) {
				throw new AssertionError(e);
			}
		}
	}

	/**
	 * int转换到IPV4 String, from Netty NetUtil
	 * 
	 * @param i
	 * @return
	 */
	public static String intToIpv4String(int i) {
		return new StringBuilder(15).append(i >> 24 & 0xFF).append('.').append(i >> 16 & 0xFF).append('.')
				.append(i >> 8 & 0xFF).append('.').append(i & 0xFF).toString();
	}

	/**
	 * Ipv4 String 转换到int
	 * 
	 * @param ipv4Str
	 * @return
	 */
	public static int ipv4StringToInt(String ipv4Str) {
		byte[] byteAddress = ip4StringToBytes(ipv4Str);
		if (byteAddress == null) {
			return 0;
		} else {
			return Ints.fromByteArray(byteAddress);
		}
	}

	/**
	 * Ipv4 String 转换到byte[]
	 * 
	 * @param ipv4Str
	 * @return 成功返回字节数组，不成功返回null
	 */
	private static byte[] ip4StringToBytes(String ipv4Str) {
		if (ipv4Str == null) {
			return null;
		}

		List<String> it = split(ipv4Str, '.', 4);
		if (it.size() != 4) {
			return null;
		}

		byte[] byteAddress = new byte[4];
		for (int i = 0; i < 4; i++) {
			int tempInt = Integer.parseInt(it.get(i));
			if (tempInt > 255) {
				return null;
			}
			byteAddress[i] = (byte) tempInt;
		}
		return byteAddress;
	}

	public static List<String> split(String str, final char separatorChar, int expectParts) {
		if (str == null) {
			return null;
		}

		final int len = str.length();
		if (len == 0) {
			return new ArrayList<String>();
		}
		final List<String> list = new ArrayList<String>(expectParts);
		int i = 0;
		int start = 0;
		boolean match = false;

		while (i < len) {
			if (str.charAt(i) == separatorChar) {
				if (match) {
					list.add(str.substring(start, i));
					match = false;
				}
				start = ++i;
				continue;
			}
			match = true;
			i++;
		}
		if (match) {
			list.add(str.substring(start, i));
		}
		return list;
	}

	private IPUtil() {
	}
}
