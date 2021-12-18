/**
 * 
 */
package com.idata.tool;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

import javax.imageio.ImageIO;

import net.coobird.thumbnailator.Thumbnails;

/**
 * @author Administrator
 *
 */
public class ImageUtil {
	
	public static BufferedImage convert(String imgBase64)
	{
		if(imgBase64 == null || "".equalsIgnoreCase(imgBase64))
		{
			return null;
		}
		
		try 
		{
			Decoder decoder = Base64.getDecoder();
			byte[] b = decoder.decode(imgBase64);
			ByteArrayInputStream in = new ByteArrayInputStream(b);
			BufferedImage image = ImageIO.read(in);
			return image;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	public static String convert3(byte[] image)
	{
		Encoder encoder = Base64.getEncoder();
		return encoder.encodeToString(image);
	}
	
	public static byte[] convert2(String imgBase64)
	{
		if(imgBase64 == null || "".equalsIgnoreCase(imgBase64))
		{
			return null;
		}
		Decoder decoder = Base64.getDecoder();
		byte[] b = decoder.decode(imgBase64);
		return b;
	}
	
	
	public static String compress(String imgBase64,long size)
	{
		if(imgBase64 == null || "".equalsIgnoreCase(imgBase64))
		{
			return "";
		}
		Decoder decoder = Base64.getDecoder();
		byte[] b = decoder.decode(imgBase64);
		b = compressPicForScale(b,size,"image");
		Encoder encoder = Base64.getEncoder();
		return encoder.encodeToString(b);
	}
	
	public static byte[] compressPicForScale(byte[] imageBytes, long desFileSize, String imageId) {
        if (imageBytes == null || imageBytes.length <= 0 || imageBytes.length < desFileSize * 1024) {
            return imageBytes;
        }
        long srcSize = imageBytes.length;
        double accuracy = getAccuracy(srcSize / 1024);
        try {
            while (imageBytes.length > desFileSize * 1024) {
                ByteArrayInputStream inputStream = new ByteArrayInputStream(imageBytes);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(imageBytes.length);
                Thumbnails.of(inputStream)
                        .scale(accuracy)
                        .outputQuality(accuracy)
                        .toOutputStream(outputStream);
                imageBytes = outputStream.toByteArray();
            }
            System.out.println("【图片压缩】imageId="+imageId+" | 图片原大小="+srcSize / 1024+"kb | 压缩后大小="+imageBytes.length / 1024+"kb");
        } catch (Exception e) {
        	e.printStackTrace();
        	System.out.println("【图片压缩】msg=图片压缩失败!");
        }
        return imageBytes;
    }
 
    /**
     * 自动调节精度(经验数值)
     *
     * @param size 源图片大小
     * @return 图片压缩质量比
     */
    private static double getAccuracy(long size) {
        double accuracy;
        if (size < 900) {
            accuracy = 0.85;
        } else if (size < 2047) {
            accuracy = 0.6;
        } else if (size < 3275) {
            accuracy = 0.44;
        } else {
            accuracy = 0.4;
        }
        return accuracy;
    }

}
