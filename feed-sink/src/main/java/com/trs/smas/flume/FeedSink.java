/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trs.client.RecordReport;
import com.trs.client.TRSConnection;
import com.trs.client.TRSConstant;
import com.trs.client.TRSException;

/**
 * 配置示例:<br/>
 * <code>
 * .type = com.trs.smas.flume.BasicTRSServerSink<br/>
 * .bufferDir = /dev/shm/flume/sink<br/>
 * .batchSize = 1000<br/>
 * .host = 192.168.200.8<br/>
 * .port = 8899<br/>
 * .username = system<br/>
 * .password = manager2013admin<br/>
 * .database = spark_test<br/>
 * .format = <REC>\n<IR_CONTENT>={body}\n
 * </code>
 * 
 * @since huangshengbo @ Apr 23, 2014 6:58:40 PM
 * 
 */
public class FeedSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(FeedSink.class);

	// private Path buffer;

	protected String host;
	protected String port;
	protected String username;
	protected String password;
	protected String database;
	protected String format;

	protected TRSConnection connection;

	protected SinkCounter sinkCounter;
	protected int batchSize;
	protected Path bufferDir;
	protected Path backupDir;

	@Override
	public synchronized void start() {
		sinkCounter.start();
		super.start();
		try {
			connection = new TRSConnection();
			connection.connect(host, port, username, password);

			connection.setBufferPath(backupDir.toString());
		} catch (TRSException e) {
			throw new RuntimeException(
					"Unable to create connection to trsserver", e);
		}
	}

	@Override
	public synchronized void stop() {
		connection.close();
		super.stop();
		sinkCounter.stop();
	}

	// public Path selectBuffer(Event e) throws IOException {
	// if (buffer == null) {
	// buffer = Files.createTempFile(bufferDir, getName(), ".trs");
	// }
	// return buffer;
	// }

	public void load(Path buffer) throws IOException {
		try {
			TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
			RecordReport report = connection.loadRecords(database, username,
					buffer.toString(), null, false);

			LOG.info("{} loaded on {}. success: " + report.lSuccessNum
					+ ", failure: " + report.lFailureNum + "",
					buffer.toString(), getName());
			sinkCounter.addToEventDrainSuccessCount(report.lSuccessNum);

			if (StringUtils.isEmpty(report.WrongFile)) {// Backup
				Files.delete(buffer);
			} else {
				backup(report.WrongFile, buffer);
			}
		} catch (TRSException e) {
			backup(e, buffer);
		} finally {
			buffer = null;
		}

	}

	protected void backup(String errorFile, Path buffer) throws IOException {
		Path errorPath = FileSystems.getDefault().getPath(errorFile);
		Files.move(errorPath,
				backupDir.resolve(String.format("%s.%s", System
						.currentTimeMillis(), errorPath.getFileName()
						.toString())), StandardCopyOption.REPLACE_EXISTING);
		Files.move(buffer, backupDir.resolve(String.format("%s.%s.%s",
				System.currentTimeMillis(), errorPath.getFileName().toString(),
				buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
	}

	protected void backup(TRSException error, Path buffer) throws IOException {
		Path errorFile = FileSystems.getDefault().getPath(
				error.getErrorString());
		if (Files.exists(errorFile)) {
			Files.move(errorFile, backupDir.resolve(String.format("%s.%s",
					System.currentTimeMillis(), errorFile.getFileName()
							.toString())), StandardCopyOption.REPLACE_EXISTING);
		} else {
			Files.write(backupDir.resolve(String.format("%s.%s",
					System.currentTimeMillis(), "ERR")), error.getErrorString()
					.getBytes(), StandardOpenOption.CREATE);
		}
		Files.move(buffer, backupDir.resolve(String.format("%s.%s",
				System.currentTimeMillis(), buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
	}

	protected void buffer(Event e){
		
	}
	
	protected void load(){
		
	}
	
	protected void fanout(){
		
	}
	
	protected void unload(){
		
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.Sink#process()
	 */
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;

		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		try {
			transaction.begin();
			int count = 0;
			for (count = 0; count < batchSize; count++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				buffer(event);
				sinkCounter.incrementEventDrainAttemptCount();
			}

			if (count == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else if (count < batchSize) {
				sinkCounter.incrementBatchUnderflowCount();
			} else {
				sinkCounter.incrementBatchCompleteCount();
			}

			if(count > 0){
				load();
				fanout();
				unload();
			}
			
			sinkCounter.addToEventDrainSuccessCount(count);
			transaction.commit();
		} catch (ChannelException e) {
			transaction.rollback();
			LOG.error(
					"Unable to get event from" + " channel "
							+ channel.getName(), e);
			return Status.BACKOFF;
		} catch (Exception ex) {
			transaction.rollback();
			LOG.error("Failed to deliver event. Exception follows.", ex);
			throw new EventDeliveryException("Failed to deliver event", ex);
		} finally {
			transaction.close();
		}

		return status;
	}

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
		format = context.getString("format", TRSFileBuilder.BODY_PLACEHOLDER)
				+ "\n";
		batchSize = context.getInteger("batchSize", 1000);

		bufferDir = FileSystems.getDefault().getPath(
				context.getString("bufferDir"));
		backupDir = FileSystems.getDefault().getPath(
				context.getString("backupDir"));

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

	static class TRSFileBuilder {

		public static final String IGNORE_HEADER_PREFIX = ".";
		public static final String BODY_PLACEHOLDER = "{body}";

		public static void append(Path path, Event e, String format)
				throws IOException {
			append(path, e, format, true);
		}

		public static void append(Path path, Event e, String format,
				boolean withEventHeaders) throws IOException {
			StringBuilder sb = new StringBuilder(format.replace(
					BODY_PLACEHOLDER, new String(e.getBody())));

			Map<String, String> headers = e.getHeaders();
			for (String key : headers.keySet()) {
				if (key.startsWith(IGNORE_HEADER_PREFIX)) {
					continue;
				}
				sb.append("<").append(key).append(">=")
						.append(headers.get(key)).append("\n");
			}

			Files.write(path, sb.toString().getBytes(),
					StandardOpenOption.APPEND);
		}
	}
}
