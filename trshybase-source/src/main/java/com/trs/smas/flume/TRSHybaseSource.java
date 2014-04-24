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

import com.trs.hybase.client.TRSConnection;
import com.trs.hybase.client.TRSException;
import com.trs.hybase.client.TRSRecord;
import com.trs.hybase.client.TRSResultSet;
import com.trs.hybase.client.params.ConnectParams;
import com.trs.hybase.client.params.SearchParams;

/**
 * <code>
 * .type = com.trs.smas.flume.TRSHybaseSource<br/>
 * .url = http://192.168.200.12:5555<br/>
 * .username = admin<br/>
 * .password = trsadmin<br/>
 * .database = news<br/>
 * .watermark = IR_LOADTIME<br/>
 * .batchSize = 1000<br/>
 * .body = <REC>\n<IR_URLTITLE>={IR_URLTITLE}\n<IR_URLNAME>={IR_URLNAME}\n<IR_CONTENT>={IR_CONTENT}\n<br/>
 * .headers = IR_GROUPNAME;IR_URLDATE<br/>
 * </code>
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
	
	/* (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	public void configure(Context context) {
		url = context.getString("url");
		username = context.getString("username");
		password = context.getString("password");
		database = context.getString("database");
		filter = context.getString("filter");
		watermarkField = context.getString("watermark");
		identifierField = context.getString("identifier");
		from = context.getString("from");
		checkpoint = FileSystems.getDefault().getPath(context.getString("checkpoint"));
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
		
		if(!StringUtils.isEmpty( context.getString("headers") )){
			headers = context.getString("headers").split(";");
		}else{
			headers = new String[0];
		}
		
		batchSize = context.getInteger("batchSize", 1000);
		
		if(sourceCounter == null){
			sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public synchronized void start() {
		//初始化watermark
		try {
			watermark = DiscreteWatermark.loadFrom(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to load watermark from" + checkpoint, e);
			throw new RuntimeException("watermark loading failed, you can delete "+ checkpoint + " and then restart.", e);
		}
		
		if(watermark == null){
			watermark = new DiscreteWatermark(watermarkField, from);
		}
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
		//保存watermark
		try {
			watermark.saveTo(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to save watermark "+ watermark +" to " + checkpoint, e);
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
		String query = StringUtils.isEmpty(watermark.getCursor()) ? filter : watermark.getApplyTo() + ": [\"" + watermark.getCursor() + "\" TO *}" + ( StringUtils.isEmpty(filter)? "" : " AND " + filter);
		
		TRSResultSet resultSet = null;
		try {
			resultSet = connection.executeSelect(this.database, query, 0, batchSize, new SearchParams().setSortMethod("+" + watermark.getApplyTo() + ";+DOCID"));			
		} catch (TRSException e) {
			LOG.error("fail to select "+database+" by "+query,e);
			return Status.BACKOFF;
		}
		if(resultSet.size() == 0){
			resultSet.close();
			return Status.BACKOFF;
		}
		for (int i = 0; buffer.size() < batchSize && i < resultSet.size(); i++) {
			resultSet.moveNext();
			try {
				TRSRecord record = resultSet.get();
				String mark = record.getString(watermark.getApplyTo());
				String id = record.getString(identifierField);
				
				if(watermark.isOverflow(mark, id)){
					sourceCounter.incrementEventReceivedCount();
					continue;
				}
				
				List<String> values = new ArrayList<String>(this.bodyArgs.size());
				
				for(String field : this.bodyArgs){
					String value = record.getString(field);
					values.add(StringUtils.defaultString(StringUtils.startsWith(value, "@") ? "//" + value : value));
				}
				Map<String,String> header = new HashMap<String,String>(this.headers.length);
				for(String key : this.headers){
					header.put(key, record.getString(key));
				}
				buffer.add(EventBuilder.withBody(String.format(body, values.toArray()).getBytes(),header));
				watermark.rise(mark, id);
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
