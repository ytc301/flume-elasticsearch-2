/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * TODO
 * @since huangshengbo @ Apr 24, 2014 12:37:57 AM
 *
 */
public class NERInterceptor implements Interceptor {

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
