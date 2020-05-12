package com.idata.test;

import java.util.List;

import com.idata.core.DataParam;
import com.idata.core.HbaseManager;
import com.idata.core.HbaseOperator;
import com.idata.core.OneDataServer;
import com.idata.core.ResultBuilder;
import com.idata.core.SuperObject;

public class Main5 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		OneDataServer.init();
		String constring = "P4Yz8hCxq7iSUF-qhLr7wJKAI7zlGHiHW0C2TgP35JGGQ-tvFC_FuNyJ7dUKP7rZha4I60LtrDY5HZ9rMPDOfA";
		//HbaseManager.importData(constring, "student", null, null, "student3", true);
		HbaseOperator op = new HbaseOperator();
		DataParam filter  = new DataParam();
		filter.setType("hbase");
		filter.setLayer("student3");
		filter.setOperation("query");
		filter.setQueryfields("age");
		filter.setQueryoperates(">");
		filter.setKeywords("7.0");
		
		List<SuperObject> os = op.query(filter);
		System.out.println(ResultBuilder.object2string(os,"json"));;
	}

}
