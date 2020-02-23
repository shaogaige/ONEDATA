/**
 * ClassName:PhoneNumCoder.java
 * Date:2018年6月26日
 */
package com.idata.tool;

/**
 * Creater:SHAO Gaige
 * Description:手机号编码器
 * Log:
 */
public class PhoneNumCoder implements ICoder{
	
	private NumberCoder numberCoder = new NumberCoder();
	
	/**
	 * 编码
	 * @param phone
	 * @return String
	 */
	public String code(String phone)
	{
		if(phone == null || "".equalsIgnoreCase(phone))
		{
			return "";	
		}
		String reverse = new StringBuilder(phone).reverse().toString();
		return numberCoder.code(reverse);
	}
	
	/**
	 * 解码
	 * @param phone
	 * @return String
	 */
	public String uncode(String phone)
	{
		if(phone == null || "".equalsIgnoreCase(phone))
		{
			return "";	
		}
		String reverse = numberCoder.uncode(phone);
		return new StringBuilder(reverse).reverse().toString();
	}

}
