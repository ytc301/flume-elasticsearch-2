package com.trs.smas.flume;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.Lists;

public class TimeFilterInterceptor implements Interceptor {
	private String filter;
	private String format;
	private int range;

	private DateTimeFormatter formatter;

	public TimeFilterInterceptor(String filter, String format, int range) {
		this.filter = filter;
		this.range = range;
		this.format = format;
	}

	public void close() {
	}

	public void initialize() {
		formatter = DateTimeFormat.forPattern(format);
	}

	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		if (formatter.parseDateTime(headers.get(filter)).plusDays(range)
				.isAfterNow()) {
			return event;
		} else {
			return null;
		}
	}

	public List<Event> intercept(List<Event> events) {
		List<Event> out = Lists.newArrayList();
		for (Event event : events) {
			Event outEvent = intercept(event);
			if (outEvent != null) {
				out.add(outEvent);
			}
		}
		return out;
	}

	public static class Builder implements Interceptor.Builder {

		private String filter;
		private String format;
		private int range;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context
		 * )
		 */
		public void configure(Context context) {
			filter = context.getString("filter");
			format = context.getString("format");
			range = context.getInteger("range", 7);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		public Interceptor build() {
			return new TimeFilterInterceptor(filter, format, range);
		}
	}
}
