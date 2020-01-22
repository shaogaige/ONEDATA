package com.idata.test;

import com.idata.core.SuperObject;

public class MainT1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SuperObject so = new SuperObject();
		String jsons = "{\r\n" + 
				"  \"type\": \"Feature\",\r\n" + 
				"  \"geometry\": {\r\n" + 
				"    \"type\": \"Point\",\r\n" + 
				"    \"coordinates\": [125.6, 10.1]\r\n" + 
				"  },\r\n" + 
				"  \"properties\": {\r\n" + 
				"    \"name\": \"Dinagat Islands\",\"age\":15,\"tt\":false,\"rr\":\"erere\"\r\n" + 
				"  }\r\n" + 
				"}";
		so.setJSONString(jsons);
		System.out.println(so.getGeo_wkt());
		System.out.println(so.getJSONString(""));
		System.out.println(so.getJSONString("geojson"));

	}

}
