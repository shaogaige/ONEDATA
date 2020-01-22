package com.idata.tool;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

public class IPAddressUtil {
	
	public static List<String> localIP;

	/**
	 * 获取本地IP地址
	 * @return String
	 */
	public static String getIPAddress() {
		try {
			InetAddress candidateAddress = null;
			// 遍历所有的网络接口 
			for (Enumeration<?> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
				NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
				// 在所有的接口下再遍历IP
				for (Enumeration<?> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
					InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
					if (!inetAddr.isLoopbackAddress()) {
						// 排除loopback类型地址
						if (inetAddr.isSiteLocalAddress()) {
							// 如果是site-local地址，就是它了
							return inetAddr.getHostAddress();
						} else if (candidateAddress == null) {
							// site-local类型的地址未被发现，先记录候选地址
							candidateAddress = inetAddr;
						}
					}
				}
			}
			if (candidateAddress != null) {
				return candidateAddress.getHostAddress();
			}
			// 如果没有发现 non-loopback地址.只能用最次选的方案
			InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
			return jdkSuppliedAddress.getHostAddress();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "127.0.0.1";
	}
	
	public static List<String> getLocalIP()
	{
		if(localIP != null)
		{
			return localIP;
		}
		else
		{
			localIP = new ArrayList<String>();
		}
		try {
			InetAddress candidateAddress = null;
			// 遍历所有的网络接口
			for (Enumeration<?> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
				NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
				// 在所有的接口下再遍历IP
				for (Enumeration<?> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
					InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
					if (!inetAddr.isLoopbackAddress()) {
						// 排除loopback类型地址
						if (inetAddr.isSiteLocalAddress()) {
							// 如果是site-local地址，就是它了
							localIP.add(inetAddr.getHostAddress());
						} else if (candidateAddress == null) {
							// site-local类型的地址未被发现，先记录候选地址
							candidateAddress = inetAddr;
						}
					}
				}
			}
			//if (candidateAddress != null) {
			//	localIP.add(candidateAddress.getHostAddress());
			//}
			// 如果没有发现 non-loopback地址.只能用最次选的方案
			//InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
			//localIP.add(jdkSuppliedAddress.getHostAddress());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return localIP;
	}
	
	/**
	 * 获取客户端的IP地址
	 * @param request
	 * @return
	 */
	public static String getClientIPAddress(HttpServletRequest request)
	{
		String ipAddress = request.getHeader("x-forwarded-for");
		if(ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
		    ipAddress = request.getHeader("Proxy-Client-IP");
		}
		if (ipAddress == null || ipAddress.length() == 0 || "unknow".equalsIgnoreCase(ipAddress)) {
		    ipAddress = request.getHeader("WL-Proxy-Client-IP");
	    }
		if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
		    ipAddress = request.getRemoteAddr();
		    if(ipAddress.equals("127.0.0.1") || ipAddress.equals("0:0:0:0:0:0:0:1")){
            //根据网卡获取本机配置的IP地址
	        InetAddress inetAddress = null;
		try {
	            inetAddress = InetAddress.getLocalHost();
            } catch (Exception e) {
                e.printStackTrace();
            }
            ipAddress = inetAddress.getHostAddress();
           }
     }
      //对于通过多个代理的情况，第一个IP为客户端真实的IP地址，多个IP按照','分割
     if(null != ipAddress && ipAddress.length() > 15){
        //"***.***.***.***".length() = 15
     if(ipAddress.indexOf(",") > 0){
            ipAddress = ipAddress.substring(0, ipAddress.indexOf(","));
        }
     }
        return ipAddress;
	}
}
