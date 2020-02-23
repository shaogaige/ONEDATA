/**
 * ClassName:IDataDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.idata.core.DataParam;
import com.idata.core.SuperObject;

/**
 * Creater:SHAO Gaige
 * Description:数据源驱动接口
 * Log:
 */
public interface IDataDriver {
	
	public static Map<String,Long> resultSize = new ConcurrentHashMap<String,Long>();
	
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
	public List<SuperObject> query(DataParam param);
	
	/**
	 * 统计分析
	 * @param DataParam
	 * @return String
	 */
	public List<SuperObject> group(DataParam param);
	
	/**
	 * 获取元数据
	 * @param DataParam
	 * @return String
	 */
	public List<SuperObject> getMeta(DataParam param);
	
	/**
	 * 是否支持此类型
	 * @return boolean
	 */
	public boolean isSupport();
}
