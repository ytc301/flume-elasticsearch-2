/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

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

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;

/**
 * 
 * @since huangshengbo @ Apr 23, 2014 6:58:40 PM
 * 
 */
public abstract class AbstractTRSServerSink extends AbstractSink implements
		Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(AbstractTRSServerSink.class);

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
		initDB();
	}

	@Override
	public synchronized void stop() {
		if(connection.isValid()){
			connection.close();
		}
		super.stop();
		sinkCounter.stop();
	}

	public void initDB() {
		try {
			connection = new TRSConnection();
			connection.connect(host, port, username, password);
			connection.setBufferPath(backupDir.toString());
		} catch (TRSException e) {
			throw new RuntimeException(
					"Unable to create connection to trsserver", e);
		}
	}

	public abstract Path selectBuffer(Event e) throws IOException;

	public abstract void load() throws IOException;

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
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		error.printStackTrace(pw);
		pw.flush();
		sw.flush();

		Files.write(backupDir.resolve(String.format("%s.%s",
				System.currentTimeMillis(), "ERR")), sw.toString().getBytes(),
				StandardOpenOption.CREATE);

		Files.move(buffer, backupDir.resolve(String.format("%s.%s",
				System.currentTimeMillis(), buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
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
			int i = 0;
			for (i = 0; i < batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				TRSFileBuilder.append(selectBuffer(event), event, format);
				sinkCounter.incrementEventDrainAttemptCount();
			}

			if (i == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else if (i < batchSize) {
				sinkCounter.incrementBatchUnderflowCount();
			} else {
				sinkCounter.incrementBatchCompleteCount();
			}

			if (i > 0) {
				load();
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
}
