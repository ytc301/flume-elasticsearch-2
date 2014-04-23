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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.trs.client.TRSException;

/**
 * .database = {.group}_{.month}
 * @since huangshengbo @ Apr 21, 2014 5:43:24 PM
 *
 */
public class MultiplexTRSServerSink extends AbstractSink implements
		Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(MultiplexTRSServerSink.class);
	
	private String host;
	private String port;
	private String username;
	private String password;
	private String database;
	private List<String> databaseArgs;
	private int batchSize;
	private SinkCounter sinkCounter;
	
	private Path bufferDir;
	private Path backupDir;
	
	private TRSConnection connection;
	
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
	
	protected Path select(Event event, Map<String, Path> buffers) throws IOException{
		Map<String,String> headers = event.getHeaders();
		List<String> values = new ArrayList<String>(this.databaseArgs.size());
		for(String name : this.databaseArgs){
			values.add(headers.get(name));
		}
		String key = String.format(database, values.toArray());
		if(!buffers.containsKey(key)){
			Path path = Files.createTempFile(bufferDir, getName()+"-"+key, ".trs");
			buffers.put(key, path);
		}
		return buffers.get(key);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;

		Channel channel = getChannel();
		Map<String, Path> buffers = new HashMap<String,Path>();
		Transaction transaction = channel.getTransaction();
		try {
			transaction.begin();
			int i = 0;
			for (i = 0; i < batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				
				Files.write(select(event, buffers), event.getBody(), StandardOpenOption.APPEND);
			}

			if (i == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else if (i < batchSize)  {
				sinkCounter.incrementBatchUnderflowCount();
			}else{
				sinkCounter.incrementBatchCompleteCount();
			}
			sinkCounter.addToEventDrainAttemptCount(i);
			
			for(String db : buffers.keySet()){
				Path bufferFile = buffers.get(db);
				try{
					RecordReport report = connection.loadRecords(db, username, bufferFile.toString(), null, false);
					sinkCounter.addToEventDrainSuccessCount(report.lSuccessNum);
					LOG.info("{} loaded on {}. success: "+ report.lSuccessNum +", failure: "+report.lFailureNum + "", bufferFile.toString(), getName());
					if (!StringUtils.isEmpty(report.WrongFile)) {// Backup
						Path errorFile = FileSystems.getDefault().getPath(
								report.WrongFile);
						Files.move(errorFile, backupDir.resolve(String.format(
								"%s.%s", System.currentTimeMillis(), 
								errorFile.getFileName().toString())),
								StandardCopyOption.REPLACE_EXISTING);
						Files.move(bufferFile, backupDir.resolve(String.format(
								"%s.%s.%s", System.currentTimeMillis(),
								errorFile.getFileName().toString(), 
								bufferFile.getFileName().toString())),
								StandardCopyOption.REPLACE_EXISTING);
					}else {
						Files.delete(bufferFile);
					}	
				}catch(TRSException e){
					Path errorFile = FileSystems.getDefault().getPath(e.getErrorString());
					if(Files.exists(errorFile)){
						Files.move(errorFile, backupDir.resolve(String.format("%s.%s", System.currentTimeMillis(), errorFile.getFileName().toString())), StandardCopyOption.REPLACE_EXISTING);
					} else {
						Files.write(backupDir.resolve(String.format("%s.%s", System.currentTimeMillis(), "ERR")), e.getErrorString().getBytes(), StandardOpenOption.CREATE);
					}
					Files.move(bufferFile, backupDir.resolve(String.format("%s.%s",
							System.currentTimeMillis(), 
							bufferFile.getFileName().toString())),
							StandardCopyOption.REPLACE_EXISTING);
				}
			}

			transaction.commit();
		} catch (ChannelException e) {
			transaction.rollback();
			LOG.error(
					"Unable to get event from channel "
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

	/* (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		host = context.getString("host");
		port = context.getString("port", "8888");
		username = context.getString("username", "system");
		password = context.getString("password", "manager");
		
		database = context.getString("database");
		databaseArgs = new ArrayList<String>(10);
		Pattern pattern = Pattern.compile("\\{(.*?)\\}");
		Matcher matcher = pattern.matcher(database);
		while(matcher.find()){
			databaseArgs.add(matcher.group(1));
		}
		for( String arg : databaseArgs){
			database = database.replace("{"+ arg+"}", "%s");
		}
		
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
