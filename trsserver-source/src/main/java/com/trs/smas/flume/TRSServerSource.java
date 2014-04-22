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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * .fields = IR_URLTITLE;IR_URLNAME;IR_CONTENT<br/>
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
	private String[] fields;
	private String[] headers;
	private String watermarkField;
	private String from;
	private Path checkpoint;

	private int batchSize;

	private TRSConnection connection;
	private Watermark watermark;

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
		from = context.getString("from");
		checkpoint = FileSystems.getDefault().getPath(
				context.getString("checkpoint"));
		fields = context.getString("fields").split(";");
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
		if (Files.exists(checkpoint)) {
			List<String> options = null;
			try {
				options = Files
						.readAllLines(checkpoint, StandardCharsets.UTF_8);
			} catch (IOException e) {
				LOG.error("watemark file init error.", e);
			}
			watermark = new Watermark(watermarkField, options.get(0),
					Long.parseLong(options.get(1)));
		} else {
			watermark = new Watermark(watermarkField, from);
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
			Files.write(checkpoint, (watermark.getCursor() + "\n" + watermark
					.getOffset()).getBytes(), StandardOpenOption.CREATE);
		} catch (IOException e) {
			LOG.error("watermark file create file.", e);
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
				: watermark.getIdentifier() + " >= " + watermark.getCursor()
						+ (StringUtils.isEmpty(filter) ? "" : " AND " + filter);
		TRSResultSet resultSet = null;
		try {
			resultSet = connection.executeSelect(this.database, query, "+" + watermark.getIdentifier(), false);
		} catch (TRSException e) {
			LOG.error("fail to select " + database + " by " + query, e);
			return Status.BACKOFF;
		}
		if (resultSet.getRecordCount() == 0) {
			resultSet.close();
			return Status.BACKOFF;
		}
		for (long i = watermark.getOffset(); i < Math.min(batchSize,
				resultSet.getRecordCount()); i++) {
			try {
				resultSet.moveTo(0, i);
				StringBuilder strBuf = new StringBuilder();
				strBuf.append("<REC>\n");
				for (String field : fields) {
					String value = resultSet.getString(field);
					strBuf.append(String.format("<%s>=%s", field,
							StringUtils.defaultString(StringUtils.startsWith(
									value, "@") ? "//" + value : value)));
					strBuf.append("\n");
				}
				Map<String, String> header = new HashMap<String, String>(
						this.headers.length);
				for (String key : this.headers) {
					header.put(key, resultSet.getString(key));
				}
				buffer.add(EventBuilder.withBody(strBuf.toString().getBytes(),
						header));
				watermark.rise(resultSet.getString(watermark.getIdentifier()));
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
