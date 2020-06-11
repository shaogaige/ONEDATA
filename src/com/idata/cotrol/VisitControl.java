/**
 * ClassName:VisitControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import java.util.Date;

import com.idata.core.OneDataServer;
import com.idata.core.VisitLogThread;


/**
 * Creater:SHAO Gaige
 * Description:接口访问信息控制器
 * Log:
 */
public class VisitControl {
	
    public static final int MAX_QUEUE_SIZE = 20; 
	
	public static final long MAX_TIME = 1000*60*30;
	
	public static void log(VisitLogModel model)
	{
		if(!OneDataServer.visitlog)
		{
			return;
		}
		//插入到队列
		OneDataServer.visitorqueue.offer(model);
		//更新访问次数
		
		//Date date = new Date();
		int size = OneDataServer.visitorqueue.size();
		if(size > MAX_QUEUE_SIZE || (model.getReqtime()-OneDataServer.visitorqueue.peek().getReqtime()>MAX_TIME))
		{
			//超限，启动线程写入数据库
			Thread thread = new Thread(new VisitLogThread());
			thread.start();
		}
	}
	
	public static class VisitLogModel {
		
		private long id;
		
		private String req_time;
		
		private long reqtime;
		
		private String req_ip;
		
		private String req_server = "ONEDATA";
		
		private String req_interface;
		
		private String req_keyword = "";
		
		private String token = "";
		
		private String usertype = "";
		
		private String results = "";
		
		private String remarks = "";
		
		public VisitLogModel()
		{
			Date date = new Date();
			reqtime = date.getTime();
			req_time = OneDataServer.df.format(date);
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public String getReq_time() {
			return req_time;
		}

		public void setReq_time(String req_time) {
			this.req_time = req_time;
		}

		public String getReq_ip() {
			return req_ip;
		}

		public void setReq_ip(String req_ip) {
			this.req_ip = req_ip;
		}

		public String getReq_server() {
			return req_server;
		}

		public void setReq_server(String req_server) {
			this.req_server = req_server;
		}

		public String getReq_interface() {
			return req_interface;
		}

		public void setReq_interface(String req_interface) {
			this.req_interface = req_interface;
		}

		public String getReq_keyword() {
			if(req_keyword.length()>150)
			{
				req_keyword = req_keyword.substring(0, 150);
			}
			return req_keyword;
		}

		public void setReq_keyword(String req_keyword) {
			if(req_keyword != null && !"".equalsIgnoreCase(req_keyword))
			{
				if(this.req_keyword != null && !"".equalsIgnoreCase(this.req_keyword))
				{
					this.req_keyword += ";";
				}
				this.req_keyword += req_keyword;
			}
		}

		public String getToken() {
			return token;
		}

		public void setToken(String token) {
			if(token != null && !"".equalsIgnoreCase(token))
			{
				this.token = token;
				this.usertype = token.substring(14, 15);
			}
		}

		public String getUsertype() {
			return usertype;
		}

		public void setUsertype(String usertype) {
			this.usertype = usertype;
		}

		public String getResults() {
			if(results.length()>100)
			{
				results = results.substring(0, 100);
			}
			return results;
		}

		public void setResults(String results) {
			this.results = results;
		}

		public String getRemarks() {
			if(remarks.length()>50)
			{
				remarks = remarks.substring(0, 50);
			}
			return remarks;
		}

		public void setRemarks(String remarks) {
			this.remarks = remarks;
		}

		public long getReqtime() {
			return reqtime;
		}

	}

}
