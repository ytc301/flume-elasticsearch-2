/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		���������������������������������������������(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

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
 * @since huangshengbo @ Apr 17, 2014 12:53:59 AM
 * 
 */
public class BasicTRSServerSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(BasicTRSServerSink.class);

	private String host;
	private String port;
	private String username;
	private String password;
	private String database;
	private String format;

	private TRSConnection connection;

	private SinkCounter sinkCounter;
	private int batchSize;
	private Path bufferDir;
	private Path backupDir;

	@Override
	public synchronized void start() {
		sinkCounter.start();
		super.start();
		try {
			connection = new TRSConnection();
			connection.connect(host, port, username, password);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.Sink#process()
	 */
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;

		Channel channel = getChannel();
		Path batch = null;
		try {
			batch = Files.createTempFile(bufferDir, getName(), ".trs");
		} catch (IOException e) {
			LOG.error("Unable to create buffer at " + bufferDir.toString(), e);
			return Status.BACKOFF;
		}

		Transaction transaction = channel.getTransaction();
		try {
			transaction.begin();
			int i = 0;
			for (i = 0; i < batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				
				TRSFileBuilder.append(batch, event, format);
			}

			if (i == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
				Files.delete(batch.toAbsolutePath());
			} else {
				if (i < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(i);
				TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
				connection.setBufferPath(backupDir.toString());

				try {
					RecordReport report = connection.loadRecords(database,
							username, batch.toString(), null, false);

					LOG.info("{} loaded on {}. success: "+ report.lSuccessNum +", failure: "+report.lFailureNum + "", batch.toString(), getName());
					sinkCounter.addToEventDrainSuccessCount(report.lSuccessNum);
					
					if (!StringUtils.isEmpty(report.WrongFile)) {// Backup
						Path errorFile = FileSystems.getDefault().getPath(
								report.WrongFile);
						Files.move(errorFile, backupDir.resolve(String.format(
								"%s.%s", System.currentTimeMillis(), 
								errorFile.getFileName().toString())),
								StandardCopyOption.REPLACE_EXISTING);
						Files.move(batch, backupDir.resolve(String.format(
								"%s.%s.%s", System.currentTimeMillis(),
								errorFile.getFileName().toString(), 
								batch.getFileName().toString())),
								StandardCopyOption.REPLACE_EXISTING);
					}else {
						Files.delete(batch);
					}	
				} catch (TRSException e) {
					Path errorFile = FileSystems.getDefault().getPath(e.getErrorString());
					if(Files.exists(errorFile)){
						Files.move(errorFile, backupDir.resolve(String.format("%s.%s", System.currentTimeMillis(), errorFile.getFileName().toString())), StandardCopyOption.REPLACE_EXISTING);
					} else {
						Files.write(backupDir.resolve(String.format("%s.%s", System.currentTimeMillis(), "ERR")), e.getErrorString().getBytes(), StandardOpenOption.CREATE);
					}
					Files.move(batch, backupDir.resolve(String.format("%s.%s",
							System.currentTimeMillis(), 
							batch.getFileName().toString())),
							StandardCopyOption.REPLACE_EXISTING);
				}
			}

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
		format = context.getString("format",TRSFileBuilder.BODY_PLACEHOLDER);
		batchSize = context.getInteger("batchSize", 1000);

		bufferDir = FileSystems.getDefault().getPath(
				context.getString("bufferDir"));
		backupDir = FileSystems.getDefault().getPath(
				context.getString("backupDir"));

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

}
