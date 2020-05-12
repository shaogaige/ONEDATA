/**
 * ClassName:VisitLogThread.java
 * Date:2018年8月1日
 */
package com.idata.core;

import java.util.ArrayList;
import java.util.List;

import com.idata.cotrol.VisitControl.VisitLogModel;
import com.idata.tool.PropertiesUtil;
import com.ojdbc.sql.PreparedParam;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:访问记录写入线程类
 * Log:
 */
public class VisitLogThread implements Runnable {

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		List<PreparedParam> params = new ArrayList<PreparedParam>();
		while(!OneDataServer.visitorqueue.isEmpty())
		{
			//批量导入到数据库
			VisitLogModel model = OneDataServer.visitorqueue.poll();
			PreparedParam preparedParam = new PreparedParam();
            Value value_1 = new Value();
            value_1.setString_value(model.getReq_time());
            preparedParam.addParam(1, value_1);
            Value value_2 = new Value();
            value_2.setString_value(model.getReq_ip());
            preparedParam.addParam(2, value_2);
            Value value_3 = new Value();
            value_3.setString_value(model.getReq_server());
            preparedParam.addParam(3, value_3);
            Value value_4 = new Value();
            value_4.setString_value(model.getReq_interface());
            preparedParam.addParam(4, value_4);
            Value value_5 = new Value();
            value_5.setString_value(model.getReq_keyword());
            preparedParam.addParam(5, value_5);
            Value value_6 = new Value();
            value_6.setString_value(model.getToken());
            preparedParam.addParam(6, value_6);
            Value value_7 = new Value();
            value_7.setString_value(model.getUsertype());
            preparedParam.addParam(7, value_7);
            Value value_8 = new Value();
            value_8.setString_value(model.getResults());
            preparedParam.addParam(8, value_8);
            Value value_9 = new Value();
            value_9.setString_value(model.getRemarks());
            preparedParam.addParam(9, value_9);
            
            params.add(preparedParam);
		}
        
        //REQ_TIME varchar2(50),REQ_IP varchar2(50),REQ_SERVER varchar2(20),REQ_INTERFACE varchar2(20),REQ_KEYWORD varchar2(150),TOKEN varchar2(60),USERTYPE varchar2(10),RESULTS varchar2(100),REMARKS varchar2(50)
        String sql = "insert into "+OneDataServer.TablePrefix+PropertiesUtil.getValue("VisitorLogTable")+" (REQ_TIME,REQ_IP,REQ_SERVER,REQ_INTERFACE,REQ_KEYWORD,TOKEN,USERTYPE,RESULTS,REMARKS) values(?,?,?,?,?,?,?,?,?)";
        boolean f = OneDataServer.SQLITEDBHandle.exePreparedBatchSQL(sql, params);
        //更新次数
        
        if(!f) {
        	System.out.println("访问记录批量插入到数据库失败！");
        }
	}

}
