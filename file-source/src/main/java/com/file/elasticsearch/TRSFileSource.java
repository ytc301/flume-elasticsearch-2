/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.file.elasticsearch;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
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


/**
 * <code>
 * .type = com.file.elasticsearch.TRSFileSource<br/>
 * .path = 192.168.201.2<br/>
 * .batchSize = 1000<br/>
 * .body = <REC>\n<IR_URLTITLE>={IR_URLTITLE}\n<IR_URLNAME>={IR_URLNAME}\n<IR_CONTENT>={IR_CONTENT}\n<br/>
 * .headers = IR_GROUPNAME;IR_URLDATE<br/>
 * </code>
 * 
 * @since 
 * 
 */
public class TRSFileSource extends AbstractSource implements PollableSource,
		Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TRSFileSource.class);

	private String path;

	private int batchSize;

	private SourceCounter sourceCounter;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	public void configure(Context context) {
		path = context.getString("path");

		batchSize = context.getInteger("batchSize", 1000);

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public synchronized void start() {
		
		sourceCounter.start();
		super.start();
	}

	@Override
	public synchronized void stop() {
		
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
		
		getChannelProcessor().processEventBatch(buffer);
		sourceCounter.incrementAppendBatchAcceptedCount();
		sourceCounter.addToEventAcceptedCount(buffer.size());

		return status;
	}

}
