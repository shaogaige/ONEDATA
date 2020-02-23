/**
 * ClassName:Main2.java
 * Date:2020年1月22日
 */
package com.idata.test;

import com.idata.core.DataParam;
import com.idata.cotrol.ConnectionControl;
import com.idata.cotrol.DataControl;

/**
 * Creater:SHAO Gaige
 * Description:
 * Log:
 */
public class Main2 {
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ConnectionControl cc = new ConnectionControl();
		//jdbc:sqlite://E:/shaogaige/iNote/iNoteRun/data/iNoteData.note
		String s = cc.getEncodeConStr("sqlite,E:/shaogaige/iNote/iNoteRun/data/iNoteData.note");
		System.out.println(s);
		//DataBaseHandle db = new DataBaseHandle(s);
		
		DataParam param  = new DataParam();
		param.setCon(s);
		param.setOperation("query");
		param.setLayer("noteinfo");
		DataControl dc = new DataControl();
		String ss = dc.process(param);
		System.out.println(ss);
		String sss = dc.process(param);
		System.out.println(sss);
		param.setOperation("getmeta");
		String ssss = dc.process(param);
		System.out.println(ssss);
	}

}
