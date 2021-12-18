/**
 * ClassName:SHPUtil.java
 * Date:2021年12月13日
 */
package com.idata.tool;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.filter.Filter;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


/**
 * Creater: ShaoGaige
 * Description:SHP工具类
 * Log:
 */
public class SHPUtil {
	
	/**
     * geojson转换为shp文件
     * 
     * @param jsonPath
     * @param shpPath
     * @return
     */
    public static boolean geojson2Shape(String jsonPath, String shpPath) {
        GeometryJSON gjson = new GeometryJSON();
        try 
        {
            // 读文件到Stringbuffer
            StringBuffer sb = new StringBuffer();
            BufferedReader br = null;
            try 
            {
                br = new BufferedReader(new FileReader(jsonPath));
                String str;
                while ((str = br.readLine()) != null) {// 逐行读取
                    sb.append(str + "\r\n");
                }
                br.close();
            } 
            catch (Exception e) 
            {
                System.out.println(e.getMessage());
            }

            JSONObject json = JSONObject.fromObject(sb.toString());
            JSONArray features = (JSONArray) json.get("features");
            JSONObject feature0 = JSONObject.fromObject(features.get(0).toString());
            // 获取属性名称
            @SuppressWarnings("rawtypes")
			Set properties = JSONObject.fromObject(feature0.get("properties")).keySet();
            String strType = ((JSONObject) feature0.get("geometry")).getString("type").toString();

            Class<?> geoType = null;
            switch (strType) {
            case "Point":
                geoType = Point.class;
                break;
            case "MultiPoint":
                geoType = MultiPoint.class;
                break;
            case "LineString":
                geoType = LineString.class;
                break;
            case "MultiLineString":
                geoType = MultiLineString.class;
                break;
            case "Polygon":
                geoType = Polygon.class;
                break;
            case "MultiPolygon":
                geoType = MultiPolygon.class;
                break;
            }
            // 创建shape文件对象
            File file = new File(shpPath);
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
            ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);
            // 定义图形信息和属性信息
            SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
            tb.setCRS(DefaultGeographicCRS.WGS84);
            tb.setName("shapefile");
            tb.add("the_geom", geoType);// 类型，Point/MultiPoint/LineString/MultiLineString/Polygon/MultiPolygon
            @SuppressWarnings("rawtypes")
			Iterator propertiesIter = properties.iterator();
            // 设置属性
            while (propertiesIter.hasNext()) {
                String str = (String) propertiesIter.next();
                tb.add(str, String.class);// 此处设置为string，如需修改请自行改写代码
            }

            ds.createSchema(tb.buildFeatureType());
            // 设置编码
            Charset charset = Charset.forName("GBK");
            ds.setCharset(charset);
            FileUtil.writeFile(shpPath.replace(".shp", ".cpg"), "GBK".getBytes());
            // 设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0],
                    Transaction.AUTO_COMMIT);

            for (int i = 0, len = features.size(); i < len; i++) {
                String strFeature = features.get(i).toString();
                Reader reader = new StringReader(strFeature);
                SimpleFeature feature = writer.next();
                switch (strType) {
                case "Point":
                    feature.setAttribute("the_geom", gjson.readPoint(reader));
                    break;
                case "MultiPoint":
                    feature.setAttribute("the_geom", gjson.readMultiPoint(reader));
                    break;
                case "LineString":
                    feature.setAttribute("the_geom", gjson.readLine(reader));
                    break;
                case "MultiLineString":
                    feature.setAttribute("the_geom", gjson.readMultiLine(reader));
                    break;
                case "Polygon":
                    feature.setAttribute("the_geom", gjson.readPolygon(reader));
                    break;
                case "MultiPolygon":
                    feature.setAttribute("the_geom", gjson.readMultiPolygon(reader));
                    break;
                }
                @SuppressWarnings("rawtypes")
				Iterator propertiesset = properties.iterator();
                while (propertiesset.hasNext()) {
                    String str = (String) propertiesset.next();
                    JSONObject featurei = JSONObject.fromObject(features.get(i).toString());
                    feature.setAttribute(str, JSONObject.fromObject(featurei.get("properties")).get(str));
                }
                writer.write();
            }
            writer.close();
            ds.dispose();
            return true;
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * shp转换为Geojson
     * 
     * @param shpPath
     * @return
     */
    public static boolean shape2Geojson(String shpPath, String jsonPath) {
        FeatureJSON fjson = new FeatureJSON();
        try 
        {
            StringBuffer sb = new StringBuffer();
            sb.append("{\"type\": \"FeatureCollection\",\"features\": [");

            File file = new File(shpPath);
            ShapefileDataStore shpDataStore = null;

            shpDataStore = new ShapefileDataStore(file.toURI().toURL());
            // 设置编码
            //尝试读取shp的字符集
            String charset_s = "GBK";
        	String cs = FileUtil.readFileContent(shpPath.replace(".shp", ".cpg"));
        	if(cs != null && !"".equalsIgnoreCase(cs.trim()))
        	{
        		charset_s = cs;
        	}
            Charset charset = Charset.forName(charset_s);
            shpDataStore.setCharset(charset);
            String typeName = shpDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = null;
            featureSource = shpDataStore.getFeatureSource(typeName);
            SimpleFeatureCollection result = featureSource.getFeatures();
            SimpleFeatureIterator itertor = result.features();
            StringBuffer array = new StringBuffer();
            while (itertor.hasNext()) {
                SimpleFeature feature = itertor.next();
                StringWriter writer = new StringWriter();
                fjson.writeFeature(feature, writer);
                array.append(writer.toString());
                array.append(",");
            }
            // 删除多余的逗号
            array.deleteCharAt(array.length() - 1);
            itertor.close();
            sb.append(array.toString());
            sb.append("]}");

            // 写入文件
            File f = new File(jsonPath);// 新建文件
            try 
            {
                BufferedWriter bw = new BufferedWriter(new FileWriter(f));
                bw.write(sb.toString());
                bw.flush();
                bw.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            return true;
      
        } 
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
	
	public static SimpleFeatureCollection  readShp(String path ,String charset)
	{
        return readShp(path, null,charset);
    }

    public static SimpleFeatureCollection  readShp(String path , Filter filter,String charset)
    {
        SimpleFeatureSource featureSource = readStoreByShp(path,charset);
        if(featureSource == null) return null;
        try 
        {
            return filter != null ? featureSource.getFeatures(filter) : featureSource.getFeatures() ;
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
        return null ;
    }

    public static  SimpleFeatureSource readStoreByShp(String path ,String charset){
        File file = new File(path);
        FileDataStore store;
        SimpleFeatureSource featureSource = null;
        if(charset== null || "".equalsIgnoreCase(charset))
        {
        	//尝试读取shp的字符集
        	String cs = FileUtil.readFileContent(path.replace(".shp", ".cpg"));
        	if(cs != null && !"".equalsIgnoreCase(cs.trim()))
        	{
        		charset = cs;
        	}
        	else
            {
             	charset = "GBK";
            }
        }
       
        try
        {
            store = FileDataStoreFinder.getDataStore(file);
            ((ShapefileDataStore) store).setCharset(Charset.forName(charset));
            featureSource = store.getFeatureSource();
        }
        catch (IOException e) 
        {
            e.printStackTrace();
        }
        return featureSource ;
    }

}
