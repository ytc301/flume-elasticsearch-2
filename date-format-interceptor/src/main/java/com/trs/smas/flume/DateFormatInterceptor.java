package com.trs.smas.flume;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateFormatInterceptor implements Interceptor {

	private String pattern;
	private String format;
	private String from;
	private String to;

	private DateTimeFormatter formatter;

	public DateFormatInterceptor(String pattern, String format, String from,
			String to) {
		this.format = format;
		this.pattern = pattern;
		this.from = from;
		this.to = to;
	}

	public void close() {
	}

	public void initialize() {
		formatter = DateTimeFormat.forPattern(pattern);
	}

	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		String date = formatter.parseDateTime(headers.get(from)).toString(
				DateTimeFormat.forPattern(format));
		headers.put(to, date);
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	public static class Builder implements Interceptor.Builder {

		private String format;
		private String pattern;
		private String from;
		private String to;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context
		 * )
		 */
		public void configure(Context context) {
			format = context.getString("format");
			pattern = context.getString("pattern");
			from = context.getString("from");
			to = context.getString("to", from);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		public Interceptor build() {
			return new DateFormatInterceptor(pattern, format, from, to);
		}
	}

}
