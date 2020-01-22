/**  
 * ClassName: GeometryUtils.java  
 * @date 2019年9月5日  
 */  
package com.idata.tool;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**  
 * Creater:YANG Fan
 * Description: Geometry转化工具类
 * Log:
 */
public class GeometryUtils {
	
	private WKTReader OGCWKTReader = new WKTReader();
	private Geometry gtv;
	private Geometry gv;
	
	public GeometryUtils(String maingeometry, String othergeometry) 
	{
		try 
		{
			if (maingeometry != null && !"".equalsIgnoreCase(maingeometry)) {
				gtv = OGCWKTReader.read(maingeometry);
			}
			if (othergeometry != null && !"".equalsIgnoreCase(othergeometry)) {
				gv = OGCWKTReader.read(othergeometry);
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Geometry difference() 
	{
		Geometry intersection = gtv.intersection(gv);
		return intersection;
	}
	
	public Geometry Intersects()
	{
		Geometry intersection = gtv.intersection(gv);
		return intersection;
	}
	
	public Geometry union() 
	{
		Geometry union = gtv.union(gv);
		return union;
	}
	
	public Geometry symDifference() 
	{
		Geometry intersection = gtv.intersection(gv);
		return intersection;
	}
	
	public Geometry buffer(Double distance, Integer quadrantSegments, Integer endCapStyle) 
	{
		Geometry buffer = gtv.buffer(distance, quadrantSegments, endCapStyle);
		return buffer;
	}
	
	public Geometry buffer(Double distance, Integer quadrantSegments) 
	{
		Geometry buffer = gtv.buffer(distance, quadrantSegments);
		return buffer;
	}

	public Geometry buffer(Double distance) 
	{
		Geometry buffer = gtv.buffer(distance);
		return buffer;
	}
	
	public static Geometry Intersects(Geometry maingeometry, Geometry othergeometry)
	{
		Geometry intersection = maingeometry.intersection(othergeometry);
		return intersection;
	}
	
	public static Geometry symDifference(Geometry maingeometry, Geometry othergeometry) 
	{
		Geometry intersection = maingeometry.intersection(othergeometry);
		return intersection;
	}
	
	public static Geometry buffer(Geometry maingeometry,Double distance, Integer quadrantSegments, Integer endCapStyle) 
	{
		Geometry buffer = maingeometry.buffer(distance, quadrantSegments, endCapStyle);
		return buffer;
	}

	
	public static Geometry buffer(Geometry maingeometry,Double distance, Integer quadrantSegments) 
	{
		Geometry buffer = maingeometry.buffer(distance, quadrantSegments);
		return buffer;
	}

	public static Geometry buffer(Geometry maingeometry,Double distance) 
	{
		Geometry buffer = maingeometry.buffer(distance);
		return buffer;
	}

	public static Geometry difference(Geometry maingeometry, Geometry othergeometry) 
	{
		Geometry intersection = maingeometry.intersection(othergeometry);
		return intersection;
	}

	public static Geometry union(Geometry maingeometry, Geometry othergeometry) 
	{
		Geometry union = maingeometry.union(othergeometry);
		return union;
	}

}
