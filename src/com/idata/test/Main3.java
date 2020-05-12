package com.idata.test;

import com.idata.core.DataParam;
import com.idata.cotrol.ConnectionControl;
import com.idata.cotrol.DataControl;

public class Main3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConnectionControl cc = new ConnectionControl();
		//jdbc:sqlite://E:/shaogaige/iNote/iNoteRun/data/iNoteData.note
		String s = cc.getEncodeConStr("postgresql,localhost:5432/postgres,postgres,shao123456");
		System.out.println(s);
		//DataBaseHandle db = new DataBaseHandle(s);
		
		DataParam param  = new DataParam();
		param.setCon(s);
		//query
		param.setOperation("query");
		param.setLayer("test");
		DataControl dc = new DataControl();
		String ss = dc.process(param);
		System.out.println(ss);
		//getmeta
		param.setOperation("getmeta");
		String ssss = dc.process(param);
		System.out.println(ssss);
		//add
		param.setOperation("add");
		param.setJsondata("{\"id\":22,\"name\":\"sb\",\"price\":1200}");
		String sss = dc.process(param);
		System.out.println(sss);
		//edit
		System.out.println("edit");
		param.setOperation("edit");
		param.setJsondata("{\"id\":22,\"name\":\"sb\",\"price\":120}");
		param.setQueryfields("id");
		param.setKeywords("22");
		String sss2 = dc.process(param);
		System.out.println(sss2);
		//delete
		System.out.println("delete");
		param.setOperation("delete");
		param.setQueryfields("id");
		param.setKeywords("22");
		String sss3 = dc.process(param);
		System.out.println(sss3);
		//group
		System.out.println("group");
		param.setOperation("group");
		param.setGroupfield("name");
		param.setSumfield("price");
		param.setQueryfields("");
		param.setKeywords("");
		String sss4 = dc.process(param);
		System.out.println(sss4);

	}

}
