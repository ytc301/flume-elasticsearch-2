package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

public class TRSServerWeibo2UserinfoSource extends AbstractSource implements
		PollableSource, Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TRSServerWeibo2UserinfoSource.class);

	private String host;
	private String port;
	private String username;
	private String password;
	private String weiboDB;
	private String weiboFilter;
	private String userDB;
	private String body;
	private List<String> bodyArgs;
	private String watermarkField;
	private String from;
	private Path checkpoint;

	private int batchSize;

	private TRSConnection connection;
	private OffsetWatermark watermark;

	private SourceCounter sourceCounter;

	@Override
	public void configure(Context context) {
		host = context.getString("host");
		port = context.getString("port", "8888");
		username = context.getString("username", "system");
		password = context.getString("password", "manager");
		weiboDB = context.getString("weiboDB");
		weiboFilter = context.getString("weiboFilter");
		userDB = context.getString("userDB");
		watermarkField = context.getString("watermark");
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

		batchSize = context.getInteger("batchSize", 1000);

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}

	}

	@Override
	public synchronized void start() {
		// 初始化watermark
		try {
			watermark = OffsetWatermark.loadFrom(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to load watermark from" + checkpoint, e);
			throw new RuntimeException(
					"watermark loading failed, you can delete " + checkpoint
							+ " and then restart.", e);
		}

		if (watermark == null) {
			watermark = new OffsetWatermark(watermarkField, from);
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

	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		List<Event> buffer = new ArrayList<Event>(batchSize);
		String query = StringUtils.isEmpty(watermark.getCursor()) ? weiboFilter
				: watermark.getApplyTo()
						+ " >= "
						+ watermark.getCursor()
						+ (StringUtils.isEmpty(weiboFilter) ? "" : " * "
								+ weiboFilter);
		TRSResultSet resultSet = null;
		try {
			resultSet = connection.executeSelect(this.weiboDB, query, "+"
					+ watermark.getApplyTo(), false);
		} catch (TRSException e) {
			LOG.error("fail to select " + this.weiboDB + " by " + query, e);
			return Status.BACKOFF;
		}
		if (resultSet.getRecordCount() == 0) {
			resultSet.close();
			return Status.BACKOFF;
		}

		StringBuffer userQuery = new StringBuffer();
		userQuery.append("IR_UID=(");
		for (int i = 0; i < Math.min(batchSize, resultSet.getRecordCount()); i++) {
			try {
				resultSet.moveTo(0, i);
				userQuery.append(resultSet.getString("IR_UID"));
				if (i < Math.min(batchSize, resultSet.getRecordCount()) - 1) {
					userQuery.append(", ");
				}
			} catch (TRSException e) {
				LOG.error("can not read data from resultset " + watermark, e);
			}

		}
		userQuery.append(")");

		TRSResultSet userinfo = null;
		try {
			userinfo = connection.executeSelect(this.userDB,
					userQuery.toString(), false);
		} catch (TRSException e) {
			LOG.error(
					"fail to select " + this.userDB + " by "
							+ userQuery.toString(), e);
			return Status.BACKOFF;
		}
		if (userinfo.getRecordCount() == 0) {
			userinfo.close();
			return Status.BACKOFF;
		}

		for (int i = 0; i < userinfo.getRecordCount(); i++) {
			try {
				userinfo.moveTo(0, i);

				List<String> values = new ArrayList<String>(
						this.bodyArgs.size());

				for (String field : this.bodyArgs) {
					String value = userinfo.getString(field);
					values.add(StringUtils.defaultString(StringUtils
							.startsWith(value, "@") ? "//" + value : value));
				}

				
				buffer.add(EventBuilder.withBody(
						String.format(body, values.toArray()).getBytes()));
				watermark.rise(userinfo.getString(watermark.getApplyTo()));
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
