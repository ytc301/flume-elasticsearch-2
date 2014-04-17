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


/**
 * TODO
 * 
 * @since huangshengbo @ Apr 17, 2014 12:53:59 AM
 * 
 */
public class TRSServerSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TRSServerSink.class);

	private SinkCounter sinkCounter;
	private int batchSize = 20;
	private Path bufferDir;

	@Override
	public synchronized void start() {
		sinkCounter.start();
		super.start();

	}

	@Override
	public synchronized void stop() {
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
			LOG.error("Unable to create buffer at "+bufferDir.toString(),e);
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
				Files.write(batch, event.getBody(),StandardOpenOption.APPEND);
			}


			if (i == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
				Files.delete(batch);
			} else {
				if (i < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(i);
				// TODO loadrecord client.appendBatch(batch);
			}

			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(i);
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
		batchSize = context.getInteger("batchSize", 20);
		bufferDir = FileSystems.getDefault().getPath(context.getString("bufferDir"));
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

}
