/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
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

	private CounterGroup counterGroup = new CounterGroup();;
	private int batchSize = 20;

	@Override
	public synchronized void start() {
		counterGroup.setName(this.getName());
		super.start();

	}

	@Override
	public synchronized void stop() {
		super.stop();
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
		Event event = null;
//		long eventCounter = counterGroup.get("events.success");

		try {
			transaction.begin();
			int i = 0;
			for (i = 0; i < batchSize; i++) {
				event = channel.take();
				if(event == null){
					status = Status.BACKOFF;
					break;
				}
				System.out.println(new String(event.getBody()));
				// TODO save to file
				// if (++eventCounter % logEveryNEvents == 0) {
				// logger.info("Null sink {} successful processed {} events.",
				// getName(), eventCounter);
				// }
			}
			transaction.commit();
			// TODO load
			counterGroup.addAndGet("events.success",
					(long) Math.min(batchSize, i));
			counterGroup.incrementAndGet("transaction.success");
		} catch (Exception ex) {
			transaction.rollback();
			counterGroup.incrementAndGet("transaction.failed");
			LOG.error("Failed to deliver event. Exception follows.", ex);
			throw new EventDeliveryException("Failed to deliver event: "
					+ event, ex);
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
	}

}
