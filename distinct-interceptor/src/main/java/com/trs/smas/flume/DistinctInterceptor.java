package com.trs.smas.flume;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class DistinctInterceptor implements Interceptor {
	public static final String IGNORE_HEADER_PREFIX = ".";

	private String skip;
	private String filter;
	private long ttl;

	public DistinctInterceptor(String skip, String filter, long ttl) {
		this.skip = skip;
		this.filter = filter;
		this.ttl = ttl;
	}

	public void close() {
	}

	public void initialize() {
		
	}

	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		// TODO get
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	public static class Builder implements Interceptor.Builder {

		private String skip;
		private String filter;
		private long ttl;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context
		 * )
		 */
		public void configure(Context context) {
			skip = context.getString("skip");
			filter = context.getString("filter");
			ttl = context.getLong("ttl");
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		public Interceptor build() {
			return new DistinctInterceptor(skip, filter, ttl);
		}
	}

}
