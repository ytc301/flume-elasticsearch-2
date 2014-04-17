/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.util.ArrayList;
import java.util.List;

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

import com.trs.hybase.client.TRSConnection;
import com.trs.hybase.client.TRSException;
import com.trs.hybase.client.TRSRecord;
import com.trs.hybase.client.TRSResultSet;
import com.trs.hybase.client.params.ConnectParams;
import com.trs.hybase.client.params.SearchParams;

/**
 * TODO
 * @since huangshengbo @ Apr 16, 2014 6:04:13 PM
 *
 */
public class TRSHybaseSource extends AbstractSource implements PollableSource, Configurable {
	
	private static final Logger LOG = LoggerFactory.getLogger(TRSHybaseSource.class);
	
	private String url;
	private String username;
	private String password;
	private String database;
	private String filter;
	private String[] fields;
	
	private int batchSize;
	
	private TRSConnection connection;
	private Watermark watermark;
	
	private SourceCounter sourceCounter;
	
	/* (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	public void configure(Context context) {
		url = context.getString("url");
		username = context.getString("username");
		password = context.getString("password");
		database = context.getString("database");
		filter = context.getString("filter");
		String watermarkField = context.getString("watermark");
		String from = context.getString("from");
		watermark = new Watermark(watermarkField, from);
		fields = context.getString("fields").split(";");
		batchSize = context.getInteger("batchSize", 1000);
		
		if(sourceCounter == null){
			sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public synchronized void start() {
		connection = new TRSConnection(this.url, this.username, this.password, new ConnectParams());
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
		super.stop();
		sourceCounter.stop();
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.PollableSource#process()
	 */
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		List<Event> buffer = new ArrayList<Event>(batchSize);
		String query = StringUtils.isEmpty(watermark.getCursor()) ? filter : watermark.getIdentifier() + ": [\"" + watermark.getCursor() + "\" TO *}" + ( StringUtils.isEmpty(filter)? "" : " AND " + filter);
		TRSResultSet resultSet = null;
		try {
			resultSet = connection.executeSelect(this.database, query, watermark.getOffset(), batchSize, new SearchParams().setSortMethod("+" + watermark.getIdentifier()));
		} catch (TRSException e) {
			LOG.error("fail to select "+database+" by "+query,e);
			return Status.BACKOFF;
		}
		if(resultSet.size() == 0){
			resultSet.close();
			return Status.BACKOFF;
		}
		for (int i = 0; i < Math.min(batchSize, resultSet.size()); i++) {
			resultSet.moveNext();
			try {
				TRSRecord record = resultSet.get();
				StringBuilder strBuf = new StringBuilder();
				strBuf.append("<REC>\n");
				for(String field : fields){
					strBuf.append(String.format("<%s>=%s", field, StringUtils.defaultString(record.getString(field))));
					strBuf.append("\n");
				}
				buffer.add(EventBuilder.withBody(strBuf.toString().getBytes()));
				watermark.rise(record.getString(watermark.getIdentifier()));
			} catch (TRSException e) {
				LOG.error("can not read data from resultset "+watermark,e);
				break;
			}
		}
		
		LOG.debug("{} record(s) ingested. current watermark:{}",resultSet.size(), watermark);
		getChannelProcessor().processEventBatch(buffer);
		sourceCounter.incrementAppendBatchAcceptedCount();
		sourceCounter.addToEventAcceptedCount(buffer.size());
		resultSet.close();
		return status;
	}

}
