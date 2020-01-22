/**
 * ClassName:SHAUtil.java
 * Date:2017年2月14日
 */
package com.idata.tool;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

/**
 * Creater: ShaoGaige
 * Description:SHA编码算法
 * Log:
 */
public class SHAUtil {
	
	/**
	 * SHA编码
	 * @param inStr
	 * @return String
	 */
	public static String shaEncode(String inStr) {
        MessageDigest sha = null;
        try {
            sha = MessageDigest.getInstance("SHA");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }

        byte[] byteArray = null;
		try {
			byteArray = inStr.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        byte[] md5Bytes = sha.digest(byteArray);
        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16) { 
                hexValue.append("0");
            }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }

}
