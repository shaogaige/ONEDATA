/**
 * ClassName:ICoder.java
 * Date:2018年6月26日
 */
package com.idata.tool;

/**
 * Creater:SHAO Gaige
 * Description:编码器接口
 * Log:
 */
public interface ICoder {
	
	/**
	 * 编码
	 * @param code
	 * @return String
	 */
	public abstract String code(String code);
	
	/**
	 * 解码
	 * @param uncode
	 * @return String
	 */
	public abstract String uncode(String uncode);

}
