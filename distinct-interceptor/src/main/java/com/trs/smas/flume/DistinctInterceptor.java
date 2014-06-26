package com.trs.smas.flume;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.redisson.Config;
import org.redisson.Redisson;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.trs.dev4.jdk16.utils.DateUtil;

public class DistinctInterceptor implements Interceptor {
	static {
		RocksDB.loadLibrary();
	}

	private static final Logger logger = LoggerFactory
			.getLogger(DistinctInterceptor.class);

	private final static String COUNTER = "distinct_counter";
	private final static String RECORDS = "distinct_records";

	private String skip_field;
	private String skip_value;
	private String filter;
	private String identifying;
	private String db_path;
	private String redis;
	private RocksDB db = null;
	private Options options = null;
	private Redisson redisson = null;

	public DistinctInterceptor(String skip_field, String skip_value,
			String filter, String identifying, String db_path, String redis) {
		this.skip_field = skip_field;
		this.skip_value = skip_value;
		this.filter = filter;
		this.identifying = identifying;
		this.db_path = db_path;
		this.redis = redis;
	}

	public void close() {
		if (db != null)
			db.close();
		if (options != null)
			options.dispose();
		if (redisson != null)
			redisson.shutdown();
	}

	public void initialize() {
		options = new Options().setCreateIfMissing(true);
		try {
			db = RocksDB.open(options, db_path);
		} catch (RocksDBException e) {
			logger.error("init rocksdb error. ", e);
		}

		Config config = new Config();
		config.setConnectionPoolSize(10);
		config.addAddress(redis);
		redisson = Redisson.create(config);
	}

	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();

		if (Pattern.compile(headers.get(skip_field)).matcher(skip_value).find()) {
			return event;
		}

		String now = DateUtil.date2String(new Date(), DateUtil.FMT_TRS_yMd);

		StringBuffer sb = new StringBuffer();
		for (String key : filter.split(";")) {
			sb.append(headers.get(key).trim());
		}
		try {
			byte[] value = db.get(sb.toString().getBytes("UTF-8"));
			if (value == null) {
				db.put(sb.toString().getBytes("UTF-8"), now.getBytes());
			} else {
				redisson.getAtomicLong(COUNTER + ":" + now).incrementAndGet();
				redisson.getList(RECORDS + ":" + now).add(
						headers.get(identifying));
				return null;
			}
		} catch (RocksDBException e) {
			logger.error("rocksdb search error. ", e);
		} catch (UnsupportedEncodingException e) {
			logger.error("get bytes error. ", e);
		}
		return event;
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

		private String skip_field;
		private String skip_value;
		private String filter;
		private String identifying;
		private String db_path;
		private String redis;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context
		 * )
		 */
		public void configure(Context context) {
			skip_field = context.getString("skip_field");
			skip_value = context.getString("skip_value");
			filter = context.getString("filter");
			identifying = context.getString("identifying");
			db_path = context.getString("db_path");
			redis = context.getString("redis");
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flume.interceptor.Interceptor.Builder#build()
		 */
		public Interceptor build() {
			return new DistinctInterceptor(skip_field, skip_value, filter,
					identifying, db_path, redis);
		}
	}

}