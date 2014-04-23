/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * 配置示例:
 * <code>
 * .type = com.trs.smas.flume.RegexMapperInterceptor$Builder<br/>
 * .from = IR_GROUPNAME<br/>
 * .to = .group <br/>
 * .mapping.news = ^国内新闻<br/>
 * .mapping.forum = ^国内论坛<br/>
 * .default = other<br/>
 * </code>
 * 
 * @since huangshengbo @ Apr 21, 2014 3:33:40 PM
 * 
 */
public class RegexMapperInterceptor implements Interceptor {

	private String from;
	private String to;
	private String defaultValue;
	private Map<String, String> mapping;
	private Map<Pattern, String> patternMapping;

	public RegexMapperInterceptor(String from, String to,
			Map<String, String> mapping, String defaultValue) {
		this.from = from;
		this.to = to;
		this.defaultValue = defaultValue;
		this.mapping = mapping;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.interceptor.Interceptor#initialize()
	 */
	public void initialize() {
		this.patternMapping = new HashMap<Pattern, String>();
		for (String value : mapping.keySet()) {
			this.patternMapping.put(Pattern.compile(mapping.get(value)), value);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flume.interceptor.Interceptor#intercept(org.apache.flume.Event
	 * )
	 */
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		String value = StringUtils.defaultString(headers.get(from));
		for (Pattern pattern : patternMapping.keySet()) {
			if (pattern.matcher(value).find()) {
				headers.put(to, patternMapping.get(pattern));
				return event;
			}
		}
		if (!StringUtils.isEmpty(defaultValue)) {
			headers.put(to, defaultValue);
		}
		return event;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.interceptor.Interceptor#intercept(java.util.List)
	 */
	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.interceptor.Interceptor#close()
	 */
	public void close() {
	}

	public static class Builder implements Interceptor.Builder {

		private String from;
		private String to;
		private String defaultValue;
		private Map<String, String> mapping;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context
		 * )
		 */
		public void configure(Context context) {
			from = context.getString("from");
			to = context.getString("to", from);
			defaultValue = context.getString("default");
			mapping = context.getSubProperties("mapping.");
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		public Interceptor build() {
			return new RegexMapperInterceptor(from, to, mapping, defaultValue);
		}
	}

}
