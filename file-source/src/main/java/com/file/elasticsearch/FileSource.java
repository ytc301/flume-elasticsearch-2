/**
 * Title:	flume-elasticsearch
 */
package com.file.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.trs.smas.flume.DiscreteWatermark;


/**
 * <code>
 * .type = com.file.elasticsearch.FileSource<br/>
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

	private Path checkpoint;
	private DiscreteWatermark watermark;
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
		
		checkpoint = FileSystems.getDefault().getPath(
				context.getString("checkpoint"));
		
		File fileDir = new File(path);
		files = fileDir.listFiles();

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public synchronized void start() {	
		/* 初始化watermark, 如果文件中存在，则读取，如果不存在则初始化 */
		try {
			watermark = DiscreteWatermark.loadFrom(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to load watermark from" + checkpoint, e);
			throw new RuntimeException(
					"watermark loading failed, you can delete " + checkpoint
							+ " and then restart.", e);
		}
		
		if (watermark == null) {
			watermark = new DiscreteWatermark((ArrayList<File>) Arrays.asList(this.files), 0);
		}
		
		sourceCounter.start();
		super.start();
	}

	@Override
	public synchronized void stop() {
		// 保存watermark
		try {
			watermark.saveTo(checkpoint);
		} catch (IOException e) {
			LOG.error("Unable to save watermark " + watermark + " to "
					+ checkpoint, e);
		}
		super.stop();
		sourceCounter.stop();
	}
	
	/**
	 * 检查文件夹中是否有新文件加入，如果有，则添加到filelist中
	 */
	private void checkFile() {
		File[] tmpFileList = new File(path).listFiles();
		if(watermark.getFileList().size() < tmpFileList.length) {
			for(int index = watermark.getFileList().size(); index < tmpFileList.length; index ++) 
				watermark.add(tmpFileList[index]);
		}
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
		this.checkFile();
		if(watermark.getCurrentFile() != null) {
			try{
				RandomAccessFile raf = new RandomAccessFile(watermark.getCurrentFile(), "rw");
				raf.seek(watermark.getStartPosition());
				String line = raf.readLine();
				while (line != null && buffer.size() < this.batchSize) {								  	               	
	                if(line.trim().equals("")) 
	                	continue;	            
	                
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
	            			watermark.rise(raf.getFilePointer());
	            			raf.close();
	            			break;
	            		}
	                }
	                line = raf.readLine();
	            }
				watermark.rise();
				raf.close();
			} catch(IOException e) {
				LOG.error(" file io exception. ", e);			
			} catch(Exception e) {
				LOG.error(" exception. ", e);
			}
		} else {
			status = Status.BACKOFF;
			return status;
		}
		
		
		getChannelProcessor().processEventBatch(buffer);
		sourceCounter.incrementAppendBatchAcceptedCount();
		sourceCounter.addToEventAcceptedCount(buffer.size());

		return status;
	}

}
