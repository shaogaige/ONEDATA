/**
 * ClassName:IPAddressCoder.java
 * Date:2018年6月26日
 */
package com.idata.tool;

/**
 * Creater:SHAO Gaige
 * Description:ip地址的编码器
 * Log:
 */
public class IPAddressCoder implements ICoder{
	
	private NumberCoder numberCoder = new NumberCoder();
	
	/**
	 * 编码
	 * @param ip
	 * @return String
	 */
	public String code(String ip)
	{
		long s = 0;
		String[] ips = ip.split("\\.");
		s += Long.parseLong(ips[0]) << 24;
		s += Long.parseLong(ips[1]) << 16;
		s += Long.parseLong(ips[2]) << 8;
		s += Long.parseLong(ips[3]);
		String i = String.valueOf(s);
		return numberCoder.code(i);
	}
	
	/**
	 * 解码
	 * @param ip
	 * @return String
	 */
	public String uncode(String ip)
	{
		StringBuffer ipc = new StringBuffer("");
		String i = numberCoder.uncode(ip);
		long ipn = Long.parseLong(i);
		// 直接右移24位  
        ipc.append(String.valueOf((ipn >>> 24)));  
        ipc.append(".");
        // 将高8位置0，然后右移16位  
        ipc.append(String.valueOf((ipn & 0x00FFFFFF) >>> 16));  
        ipc.append(".");  
        // 将高16位置0，然后右移8位  
        ipc.append(String.valueOf((ipn & 0x0000FFFF) >>> 8));  
        ipc.append(".");  
        // 将高24位置0  
        ipc.append(String.valueOf((ipn & 0x000000FF)));  
        return ipc.toString();
	}

}
