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
import com.trs.ckm.soap.RuleCATField;
import com.trs.ckm.soap.TrsCkmSoapClient;

/**
 * .content = IR_URLTITLE;IR_CONTENT
 * .to = LOCATION
 * .model = 地区分类
 * 
 * @since huangshengbo @ Apr 24, 2014 12:37:21 AM
 *
 */
public class ClassifyInterceptor implements Interceptor {

	public static final String BODY_PLACEHOLDER = "IR_CONTENT";
	
	private TrsCkmSoapClient client; 
	private String modelName;
	private List<String> classifyFields;
	private String to;
	
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
		// TODO Auto-generated method stub
		Map<String,String> headers = event.getHeaders();
		List<RuleCATField> fields = new ArrayList<RuleCATField>(classifyFields.size());
		for( String f : classifyFields ){
			if(BODY_PLACEHOLDER.equals(f)){
				fields.add( new RuleCATField( f, new String(event.getBody() ) ));
			}else{
				fields.add( new RuleCATField( f, headers.get(f) ));
			}
		}
		
		try {
			String result = client.RuleCATClassifyText(modelName, fields.toArray(new RuleCATField[0]));
			headers.put(to, result);
		} catch (CkmSoapException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return event;
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

		/* (non-Javadoc)
		 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
		 */
		@Override
		public void configure(Context context) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		@Override
		public Interceptor build() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

}
