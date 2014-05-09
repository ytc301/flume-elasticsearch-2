/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trs.dev4.jdk16.utils.DateUtil;
import com.trs.hybase.client.TRSConnection;
import com.trs.hybase.client.TRSException;
import com.trs.hybase.client.TRSExport;
import com.trs.hybase.client.TRSRecord;
import com.trs.hybase.client.params.ConnectParams;
import com.trs.hybase.client.params.SearchParams;

/**
 * <code>
 * .type = com.trs.smas.flume.TRSHybaseSource<br/>
 * .url = http://192.168.200.12:5555<br/>
 * .username = admin<br/>
 * .password = trsadmin<br/>
 * .database = news<br/>
 * .watermark = IR_LOADTIME<br/>
 * .batchSize = 1000<br/>
 * .body = <REC>\n<IR_URLTITLE>={IR_URLTITLE}\n<IR_URLNAME>={IR_URLNAME}\n<IR_CONTENT>={IR_CONTENT}\n<br/>
 * .headers = IR_GROUPNAME;IR_URLDATE<br/>
 * .from = 2014/04/25 13:30:00<br/>
 * .ngram = 10<br/>
 * .delay = -10<br/>
 * .range = 30
 * </code>
 * 
 * @since huangshengbo @ Apr 16, 2014 6:04:13 PM
 * 
 */
public class TRSHybaseSource extends AbstractSource implements PollableSource,
		Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TRSHybaseSource.class);

	private static final String FMT_HYBASE_yMdHm = "yyyy/MM/dd HH:mm:ss";
	private static final String FMT_HYBASE_yMd = "yyyy/MM/dd";

	private String url;
	private String username;
	private String password;
	private String database;
	private String filter;
	private String body;
	private List<String> bodyArgs;
	private String[] headers;
	private String watermark;
	private int delay;
	private int range;
	private String partition;
	private Date from;
	private int step;
	private Path checkpoint;
	private Path exportTmp;

	private int batchSize;
	private List<Event> buffer;

	private TRSConnection connection;

	private SourceCounter sourceCounter;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	public void configure(Context context) {
		url = context.getString("url");
		username = context.getString("username");
		password = context.getString("password");
		database = context.getString("database");
		filter = context.getString("filter");
		watermark = context.getString("watermark");
		delay = context.getInteger("delay", -10);
		range = context.getInteger("range", 30);
		partition = context.getString("partition");
		try {
			from = DateUtils.parseDate(context.getString("from"),
					new String[] { FMT_HYBASE_yMdHm });
		} catch (ParseException e) {
			LOG.error("parse from date error! ", e);
		}
		step = context.getInteger("step", 30);
		checkpoint = FileSystems.getDefault().getPath(
				context.getString("checkpoint"));
		exportTmp = FileSystems.getDefault().getPath(
				context.getString("exportTmp"));
		body = context.getString("body");
		bodyArgs = new ArrayList<String>();
		Pattern pattern = Pattern.compile("\\{(.*?)\\}");
		Matcher matcher = pattern.matcher(body);
		while (matcher.find()) {
			bodyArgs.add(matcher.group(1));
		}
		for (String arg : bodyArgs) {
			body = body.replace("{" + arg + "}", "%s");
		}

		if (!StringUtils.isEmpty(context.getString("headers"))) {
			headers = context.getString("headers").split(";");
		} else {
			headers = new String[0];
		}

		batchSize = context.getInteger("batchSize", 1000);

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public synchronized void start() {
		connection = new TRSConnection(this.url, this.username, this.password,
				new ConnectParams());

		if (Files.exists(checkpoint)) {
			try {
				from = DateUtils.parseDate(
						Files.readAllLines(checkpoint, StandardCharsets.UTF_8)
								.get(0), new String[] { FMT_HYBASE_yMdHm });
			} catch (ParseException e) {
				LOG.error("parse from failed! ", e);
			} catch (IOException e) {
				LOG.error("read checkpoint file failed! ", e);
			}
		}

		sourceCounter.start();
		super.start();
	}

	@Override
	public synchronized void stop() {
		try {
			connection.close();
		} catch (TRSException e) {
			LOG.warn("closing hybase connection failed.", e);
		}

		try {
			Files.write(checkpoint, DateUtil
					.date2String(from, FMT_HYBASE_yMdHm).getBytes(),
					StandardOpenOption.CREATE);
		} catch (IOException e) {
			LOG.error("checkpoint save failed! ", e);
		}

		super.stop();
		sourceCounter.stop();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.PollableSource#process()
	 */
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;

		if (from != null
				&& from.before(DateUtils.addMinutes(new Date(), delay))) {

			buffer = new ArrayList<Event>(batchSize);

			String query = "("
					+ watermark
					+ ":[\""
					+ DateUtil.date2String(from, FMT_HYBASE_yMdHm)
					+ "\" TO \""
					+ DateUtil.date2String(DateUtils.addSeconds(from, step),
							FMT_HYBASE_yMdHm)
					+ "\"})"
					+ (StringUtils.isEmpty(filter) ? "" : " AND (" + filter
							+ ")");

			String rangeFilter = partition
					+ ":["
					+ DateUtil.date2String(DateUtils.addDays(from, -range),
							FMT_HYBASE_yMd)
					+ " TO "
					+ DateUtil.date2String(DateUtils.addDays(from, range),
							FMT_HYBASE_yMd) + "]";

			SearchParams params = new SearchParams();
			params.setProperty("search.range.filter", rangeFilter);

			TRSExport export = new TRSExport(connection,
					new TRSExport.IRecordListener() {

						@Override
						public boolean onRecord(TRSRecord record)
								throws Exception {
							List<String> values = new ArrayList<String>(
									bodyArgs.size());

							for (String field : bodyArgs) {
								String value = record.getString(field);
								values.add(StringUtils
										.defaultString(StringUtils.startsWith(
												value, "@") ? "//" + value
												: value));
							}
							Map<String, String> header = new HashMap<String, String>(
									headers.length);
							for (String key : headers) {
								header.put(key, record.getString(key));
							}
							buffer.add(EventBuilder.withBody(
									String.format(body, values.toArray())
											.getBytes(), header));
							return true;
						}

						@Override
						public void onEnd() throws Exception {

						}

						@Override
						public void onBegin() throws Exception {

						}

						@Override
						public String getDataDir() {
							return exportTmp.toString();
						}
					});

			long succeed = 0;

			try {
				succeed = export.export(database, query, 0, Integer.MAX_VALUE,
						params);
			} catch (TRSException e) {
				LOG.error("fail to export " + database + " by " + query, e);
				return Status.BACKOFF;
			}

			from = DateUtils.addSeconds(from, step);

			LOG.info(
					"{} record(s) ingested. current query {} and range filter {}.",
					new Object[] { succeed, query, rangeFilter });
			getChannelProcessor().processEventBatch(buffer);
			sourceCounter.incrementAppendBatchAcceptedCount();
			sourceCounter.addToEventAcceptedCount(buffer.size());
		}
		return status;
	}
}
