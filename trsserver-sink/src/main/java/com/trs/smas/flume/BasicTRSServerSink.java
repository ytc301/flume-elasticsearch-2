/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
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
 * @since huangshengbo @ Apr 23, 2014 11:19:21 PM
 *
 */
public class BasicTRSServerSink extends AbstractTRSServerSink {
	
	private static final Logger LOG = LoggerFactory.getLogger(BasicTRSServerSink.class);
	
	private Path buffer;
	
	/* (non-Javadoc)
	 * @see com.trs.smas.flume.AbstractTRSServerSink#selectBuffer(org.apache.flume.Event)
	 */
	@Override
	public Path selectBuffer(Event e) throws IOException {
		if(buffer == null){
			buffer = Files.createTempFile(bufferDir, getName(), ".trs");
		}
		return buffer;
	}

	/* (non-Javadoc)
	 * @see com.trs.smas.flume.AbstractTRSServerSink#load()
	 */
	@Override
	public void load() throws IOException {
		if (!connection.isValid() || connection.close()) {
			super.initDB();
		}
		try {
			TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
			RecordReport report = connection.loadRecords(database,
					username, buffer.toString(), null, false);

			LOG.info("{} loaded on {}. success: "+ report.lSuccessNum +", failure: "+report.lFailureNum + "", buffer.toString(), getName());
			sinkCounter.addToEventDrainSuccessCount(report.lSuccessNum);
			
			if (StringUtils.isEmpty(report.WrongFile)) {// Backup
				Files.delete(buffer);
			}else {
				backup(report.WrongFile, buffer);
			}	
		} catch (TRSException e) {
			backup(e, buffer);
		}finally{
			buffer = null;
		}
	}
}
