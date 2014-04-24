/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.trs.ckm.soap.CkmSoapException;
import com.trs.ckm.soap.TrsCkmSoapClient;

/**
 * .content = {IR_URLTITLE}\n{body}
 * .to = MD5
 * .url
 * .username
 * .password
 * @since huangshengbo @ Apr 24, 2014 12:36:30 AM
 *
 */
public class MD5Interceptor implements Interceptor {
	
	public static final String BODY_PLACEHOLDER = "body";
	
	private TrsCkmSoapClient client;
	private String content;
	private String to;
	private List<String> fromArgs;
	
	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#initialize()
	 */
	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#intercept(org.apache.flume.Event)
	 */
	@Override
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		List<String> values = new ArrayList<String>(this.fromArgs.size());
		for (String name : this.fromArgs) {
			if(BODY_PLACEHOLDER.equals(name)){
				values.add(new String(event.getBody()));
			}else{
				values.add(headers.get(name));
			}
		}
		
		try {
			String md5 = client.SimMD5GenerateTheme(String.format(content, values.toArray())).getdigest() ;
			headers.put(to, md5);
		} catch (CkmSoapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#intercept(java.util.List)
	 */
	@Override
	public List<Event> intercept(List<Event> events) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
	
	public static class Builder implements Interceptor.Builder {

		private String content;
		private String to;
		
		/* (non-Javadoc)
		 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
		 */
		@Override
		public void configure(Context context) {
			content = context.getString("content");
			to = context.getString("to");
		}

		/* (non-Javadoc)
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		@Override
		public Interceptor build() {
			return new MD5Interceptor();
		}
		
	}

}
