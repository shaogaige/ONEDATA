/**
 * ClassName:GetParticulars.java
 * Date:2019年7月1日
 */
package com.idata.tool;

import java.util.Properties;

import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * 获取计算机信息
 */
public class LocalParameter
{

	/*
	 * public static void main(String[] args) throws SigarException {
	 * System.out.println(System.currentTimeMillis()); String[] java = getJava();
	 * String[] cpu = getCPU(); String[] runMemory = getRunMemory(); String[][]
	 * memory = getMemory(); System.out.println(System.currentTimeMillis()); }
	 */

	/**
	 * 获取JAVA信息,以及操作系统
	 * 
	 * @return [0]Java的运行环境版本,[1]Java的运行环境供应商,[2]JVM可以使用的总内存,[3]JVM可以使用的剩余内存,[4]操作系统的名称,[5]操作系统的构架
	 * @throws SigarException
	 */
	public static String[] getJava()
	{
		Runtime r = Runtime.getRuntime();
		Properties props = System.getProperties();

		return new String[] { props.getProperty("java.version"), props.getProperty("java.vendor"), r.totalMemory() + "",
				r.freeMemory() + "", props.getProperty("os.name"), props.getProperty("os.arch") };
	}

	/**
	 * 获取CPU信息
	 * 
	 * @return [0]CPU个数,[1]MHz,[2]CPU生产商,[3]CPU类别[4]CPU缓存数量
	 * @throws SigarException
	 */
	public static String[] getCPU()
	{
		try
		{
			Sigar.load();
			Sigar sigar = new Sigar();	
			CpuInfo infos[] = sigar.getCpuInfoList();
			int length = infos.length;
			CpuInfo info = infos[0];
			return new String[] { length + "", info.getMhz() + "", info.getVendor(), info.getModel(),
					info.getCacheSize() + "" };
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new String[] { "4", "3000", "Intel", "Core(TM) i5-7400 CPU @ 3.00GHz", "-1" };
		}
	}

	/**
	 * 获取运行内存信息
	 * 
	 * @return [0]内存总量,[1]当前内存使用量,[2]当前内存剩余量/单位K
	 * @throws SigarException
	 */
	public static String[] getRunMemory()
	{
		try
		{
			Sigar.load();
			Sigar sigar = new Sigar();
			Mem mem = sigar.getMem();
			return new String[] { mem.getTotal() / 1024L + "", mem.getUsed() / 1024L + "", mem.getFree() / 1024L + "" };
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return new String[] { "8293836", "5885196", "2408640" };
		}
	}

	/**
	 * 获取物理内存信息
	 * 
	 * @return [0]盘符名称,[1]内存总量,[2]当前内存使用量/单位K,[3]读取速度,[4]写入速度
	 * @throws SigarException
	 */
	public static String[][] getMemory()
	{
		try
		{
			Sigar.load();
			Sigar sigar = new Sigar();
			FileSystem fslist[] = sigar.getFileSystemList();
			String[][] strings = new String[fslist.length][5];
			String[] trings2 = new String[] { "", "", "" };
			for (int i = 0; i < fslist.length; i++)
			{
				FileSystem fs = fslist[i];
				FileSystemUsage usage = null;
				try
				{
					usage = sigar.getFileSystemUsage(fs.getDirName());
				} catch (Exception e)
				{
					continue;
				}

				switch (fs.getType())
				{
				// TYPE_UNKNOWN ：未知
				case 0:
					break;
				// TYPE_NONE
				case 1: 
					break;
				// TYPE_LOCAL_DISK : 本地硬盘
				case 2: 
					trings2 = new String[] { fs.getDevName(), usage.getTotal() + "", usage.getUsed() + "",
							usage.getDiskReads() + "", usage.getDiskWrites() + "" };
					break;
				// TYPE_NETWORK ：网络
				case 3:
					break;
				// TYPE_RAM_DISK ：闪存
				case 4:
					break;
				// TYPE_CDROM ：光驱
				case 5:
					break;
				// TYPE_SWAP ：页面交换
				case 6:
					break;
				}

				strings[i] = trings2;

			}
			return strings;
		} catch (Exception e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return new String[][] {{"C:\\","104857596","86726420" },{"D:\\","838000636","70833692"}};
		}

	}

}
