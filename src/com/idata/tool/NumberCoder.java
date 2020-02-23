/**
 * ClassName:NumberCoder.java
 * Date:2018年6月26日
 */
package com.idata.tool;

/**
 * Creater:SHAO Gaige
 * Description:数字编码器
 * Log:
 */
public class NumberCoder implements ICoder{
	
	/**
	 * 编码
	 * @param num
	 * @return String
	 */
	public String code(String num)
	{
		String r = "";
		int length = num.length();
		for(int i=0;i<length;i++)
		{
			char s = num.charAt(i);
			if(i%2==0)
			{
				int n  = s - '0';
				n = n + 65 + i%16;
				char c = (char)n;
				r += c;
			}
			else
			{
				r += s;
			}
		}
		return r;
	}
	
	/**
	 * 解码
	 * @param num
	 * @return String
	 */
	public String uncode(String num)
	{
		String r = "";
		int length = num.length();
		for(int i=0;i<length;i++)
		{
			char s = num.charAt(i);
			if(i%2==0)
			{
				int n  = s - 65 - i%16;
				r += n;
			}
			else
			{
				r += s;
			}
		}
		return r;
	}

}
