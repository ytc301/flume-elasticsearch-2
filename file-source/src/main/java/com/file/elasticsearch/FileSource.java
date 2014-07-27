/**
 * Title:	flume-elasticsearch
 */
package com.file.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * .path = file path<br/>
 * .batchSize = 1000<br/>
 * </code>
 * 
 * @since 
 * 
 */
public class FileSource extends AbstractSource implements PollableSource,
		Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(FileSource.class);

	private String path;
	
	private File[] files;

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
		File fileDir = new File(path);
		files = fileDir.listFiles();
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
		
		HashMap<String, String> dataMap = new HashMap<String, String>();
		
		for(File file : files) {

			try{
				RandomAccessFile raf = new RandomAccessFile(file, "rw");
				String line = raf.readLine();
				while (line != null && buffer.size() < this.batchSize) {								
	               	               
	                if(line.trim().equals("")) {
	                	continue;
	                }
	                
	                if(line.equals("<REC>")) { 
	                	/* 每条记录的开始 */
	                	dataMap = new HashMap<String, String>();
	                	continue;
	                } else {
	                	/* 记录中间，需要将读取出的key和value放入map中 */
	                	Pattern pattern = Pattern.compile("<(.+?)>=(.+?|$)$");
	                	Matcher matcher = pattern.matcher(line);
	            		if(matcher.find()) {
	            			dataMap.put(matcher.group(1), matcher.group(2));
	            		}          		
	            		buffer.add(EventBuilder.withBody(null, dataMap));
	            		if(buffer.size() >= this.batchSize) {
	            			/* 如果文件没有读完，但buffer中event数量到达上限，则记录文件读取位置，并跳出循环 */
//	            			this.position = raf.getFilePointer();
	            			break;
	            		}
	                }
	                line = raf.readLine();
	            }
				raf.close();
			} catch(IOException e) {
				LOG.error(" file io exception. ", e);
				break;
			} catch(Exception e) {
				LOG.error(" exception. ", e);
				break;
			}
		}
		
		
		getChannelProcessor().processEventBatch(buffer);
		sourceCounter.incrementAppendBatchAcceptedCount();
		sourceCounter.addToEventAcceptedCount(buffer.size());

		return status;
	}

}
