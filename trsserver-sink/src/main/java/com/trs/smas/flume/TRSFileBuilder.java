/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.apache.flume.Event;

/**
 * TRS装库文件构造器
 * @since huangshengbo @ Apr 23, 2014 5:47:57 PM
 *
 */
public class TRSFileBuilder {
	
	public static final String IGNORE_HEADER_PREFIX = ".";
	public static final String BODY_PLACEHOLDER = "{body}";

	/** 
	 * 添加记录到TRS装库文件
	 * @param path
	 * @param e
	 * @throws IOException
	 * @since huangshengbo @ Apr 23, 2014 5:57:00 PM
	*/
	public static void append(Path path, Event e) throws IOException{
		append(path, e, BODY_PLACEHOLDER);
	}
	
	public static void append(Path path, Event e, String format) throws IOException{
		append(path, e, format, true);
	}
	
	public static void append(Path path, Event e, String format, boolean withEventHeaders) throws IOException{
		StringBuilder sb = new StringBuilder(format.replace(BODY_PLACEHOLDER, new String(e.getBody(), "UTF-8")));
		
		Map<String,String> headers = e.getHeaders();
		for(String key : headers.keySet()){
			if(key.startsWith(IGNORE_HEADER_PREFIX)){
				continue;
			}
			sb.append("<").append(key).append(">=").append(headers.get(key)).append("\n");
		}
		
		Files.write(path, sb.toString().getBytes("UTF-8"), StandardOpenOption.APPEND);
	}
}
