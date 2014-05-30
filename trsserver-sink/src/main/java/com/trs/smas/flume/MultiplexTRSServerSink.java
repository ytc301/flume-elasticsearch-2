/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.nio.file.Files;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trs.client.RecordReport;
import com.trs.client.TRSConnection;
import com.trs.client.TRSConstant;
import com.trs.client.TRSException;

/**
 * .database = {.group}_{.month}
 * 
 * @since huangshengbo @ Apr 23, 2014 11:42:59 PM
 * 
 */
public class MultiplexTRSServerSink extends AbstractTRSServerSink {

	private static final Logger LOG = LoggerFactory
			.getLogger(MultiplexTRSServerSink.class);

	private Map<String, Path> buffers = new HashMap<String, Path>();

	private List<String> databaseArgs;

	@Override
	public void configure(Context context) {
		super.configure(context);

		databaseArgs = new ArrayList<String>(10);
		Pattern pattern = Pattern.compile("\\{(.*?)\\}");
		Matcher matcher = pattern.matcher(database);
		while (matcher.find()) {
			databaseArgs.add(matcher.group(1));
		}
		for (String arg : databaseArgs) {
			database = database.replace("{" + arg + "}", "%s");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.trs.smas.flume.AbstractTRSServerSink#selectBuffer(org.apache.flume
	 * .Event)
	 */
	@Override
	public Path selectBuffer(Event e) throws IOException {
		Map<String, String> headers = e.getHeaders();
		List<String> values = new ArrayList<String>(this.databaseArgs.size());
		for (String name : this.databaseArgs) {
			values.add(headers.get(name));
		}
		String key = String.format(database, values.toArray());
		if (!buffers.containsKey(key)) {
			Path path = Files.createTempFile(bufferDir, getName() + "-" + key,
					".trs");
			buffers.put(key, path);
		}
		return buffers.get(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.trs.smas.flume.AbstractTRSServerSink#load()
	 */
	@Override
	public void load() throws IOException {
		if (!connection.isValid() || connection.close()) {
			super.initDB();
		}
		for (String db : buffers.keySet()) {
			Path bufferFile = buffers.get(db);
			try {
				TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
				RecordReport report = connection.loadRecords(db, username,
						bufferFile.toString(), null, false);
				sinkCounter.addToEventDrainSuccessCount(report.lSuccessNum);
				LOG.info("{} loaded on {}. success: " + report.lSuccessNum
						+ ", failure: " + report.lFailureNum + "",
						bufferFile.toString(), getName());
				if (!StringUtils.isEmpty(report.WrongFile)) {// Backup
					backup(report.WrongFile, bufferFile);
				} else {
					Files.delete(bufferFile);
				}
			} catch (TRSException e) {
				backup(e, bufferFile);
			} catch (Exception e) {
				LOG.error("Exception when load " + bufferFile, e);
			}
		}
		buffers.clear();
	}

}
