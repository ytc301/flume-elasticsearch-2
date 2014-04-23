/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
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

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;
import com.trs.client.TRSResultSet;

/**
 * <code>
 * .type = com.trs.smas.flume.TRSServerSource<br/>
 * .host = 192.168.201.2<br/>
 * .port = 8888<br/>
 * .username = system<br/>
 * .password = manager<br/>
 * .database = news<br/>
 * .watermark = IR_LOADTIME<br/>
 * .batchSize = 1000<br/>
 * .body = <REC>\n<IR_URLTITLE>={IR_URLTITLE}\n<IR_URLNAME>={IR_URLNAME}\n<IR_CONTENT>={IR_CONTENT}\n<br/>
 * .headers = IR_GROUPNAME;IR_URLDATE<br/>
 * </code>
 * 
 * @since fengwei @ Apr 22, 2014 9:34:43 PM
 * 
 */
public class TRSServerSource extends AbstractSource implements PollableSource,
		Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TRSServerSource.class);

	private String host;
	private String port;
	private String username;
	private String password;
	private String database;
	private String filter;
	private String body;
	private List<String> bodyArgs;
	private String[] headers;
	private String watermarkField;
	private String identifierField;
	private String from;
	private Path checkpoint;

	private int batchSize;

	private TRSConnection connection;
	private DiscreteWatermark watermark;

	private SourceCounter sourceCounter;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	public void configure(Context context) {
		host = context.getString("host");
		port = context.getString("port", "8888");
		username = context.getString("username", "system");
		password = context.getString("password", "manager");
		database = context.getString("database");
		filter = context.getString("filter");
		watermarkField = context.getString("watermark");
		identifierField = context.getString("identifierField");
		from = context.getString("from");
		checkpoint = FileSystems.getDefault().getPath(
				context.getString("checkpoint"));
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
		// 初始化watermark
		try {
			watermark = DiscreteWatermark.loadFrom(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to load watermark from" + checkpoint, e);
			throw new RuntimeException(
					"watermark loading failed, you can delete " + checkpoint
							+ " and then restart.", e);
		}

		if (watermark == null) {
			watermark = new DiscreteWatermark(watermarkField, from);
		}

		try {
			connection = new TRSConnection();
			connection.connect(host, port, username, password);
		} catch (TRSException e) {
			throw new RuntimeException(
					"Unable to create connection to trsserver", e);
		}
		sourceCounter.start();
		super.start();
	}

	@Override
	public synchronized void stop() {
		connection.close();
		// 保存watermark
		try {
			watermark.saveTo(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to save watermark " + watermark + " to "
					+ checkpoint, e);
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
		List<Event> buffer = new ArrayList<Event>(batchSize);
		String query = StringUtils.isEmpty(watermark.getCursor()) ? filter
				: watermark.getApplyTo() + " >= " + watermark.getCursor()
						+ (StringUtils.isEmpty(filter) ? "" : " * " + filter);
		TRSResultSet resultSet = null;
		try {
			resultSet = connection.executeSelect(this.database, query, "+"
					+ watermark.getApplyTo(), false);
		} catch (TRSException e) {
			LOG.error("fail to select " + database + " by " + query, e);
			return Status.BACKOFF;
		}
		if (resultSet.getRecordCount() == 0) {
			resultSet.close();
			return Status.BACKOFF;
		}
		for (long i = 0; buffer.size() < batchSize
				&& i < resultSet.getRecordCount(); i++) {
			try {
				resultSet.moveTo(0, i);
				String mark = resultSet.getString(watermark.getApplyTo());
				String id = resultSet.getString(identifierField);
				if (watermark.isOverflow(mark, id)) {
					continue;
				}

				List<String> values = new ArrayList<String>(this.bodyArgs.size());

				for (String field : this.bodyArgs) {
					String value = resultSet.getString(field);
					values.add(StringUtils.defaultString(StringUtils
							.startsWith(value, "@") ? "//" + value : value));
				}

				Map<String, String> header = new HashMap<String, String>(
						this.headers.length);
				for (String key : this.headers) {
					header.put(key, resultSet.getString(key));
				}
				buffer.add(EventBuilder.withBody(
						String.format(body, values.toArray()).getBytes(),
						header));
				watermark.rise(mark, id);
			} catch (TRSException e) {
				LOG.error("can not read data from resultset " + watermark, e);
				break;
			}
		}

		LOG.debug("{} record(s) ingested. current watermark:{}",
				resultSet.getRecordCount(), watermark);
		getChannelProcessor().processEventBatch(buffer);
		sourceCounter.incrementAppendBatchAcceptedCount();
		sourceCounter.addToEventAcceptedCount(buffer.size());
		resultSet.close();
		return status;
	}

}
