/**
 * ClassName:IDataDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import com.idata.core.DataParam;

/**
 * Creater:SHAO Gaige
 * Description:数据源驱动接口
 * Log:
 */
public interface IDataDriver {
	
	/**
	 * 新增记录
	 * @param DataParam
	 * @return boolean
	 */
	public boolean add(DataParam param);
	
	/**
	 * 编辑记录
	 * @param DataParam
	 * @return boolean
	 */
	public boolean edit(DataParam param);
	
	/**
	 * 删除记录
	 * @param param
	 * @return boolean
	 */
	public boolean delete(DataParam param);
	
	/**
	 * 查询过滤
	 * @param DataParam
	 * @return String
	 */
	public String query(DataParam param);
	
	/**
	 * 统计分析
	 * @param DataParam
	 * @return String
	 */
	public String group(DataParam param);

}
