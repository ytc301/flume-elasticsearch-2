/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.codehaus.jackson.map.ObjectMapper;
import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.core.RAtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trs.client.ClassInfo;
import com.trs.client.RecordReport;
import com.trs.client.TRSConnection;
import com.trs.client.TRSConstant;
import com.trs.client.TRSDataBase;
import com.trs.client.TRSException;
import com.trs.client.TRSResultSet;
import com.trs.dev4.jdk16.utils.StringHelper;
import com.trs.smas.flume.db.impl.TRSConnectionPool;
import com.trs.smas.flume.instrumentation.FeedCounter;
import com.trs.smas.flume.type.SearchFeed;
import com.trs.smas.flume.type.StatisticFeed;

/**
 * 配置示例:<br/>
 * <code>
 * .type = com.trs.smas.flume.BasicTRSServerSink<br/>
 * .bufferDir = /dev/shm/flume/sink<br/>
 * .batchSize = 1000<br/>
 * .dbHost = 192.168.200.8<br/>
 * .dbPort = 8899<br/>
 * .dbUsername = system<br/>
 * .dbPassword = manager2013admin<br/>
 * .dbTemplate = chuantong_<br/>
 * .format = {body}<br/>
 * .redis = 127.0.0.1<br/>
 * .subscribersKey = subscribers<br/>
 * .kafka = 127.0.0.1
 * </code>
 * 
 * @since huangshengbo @ Apr 23, 2014 6:58:40 PM
 * 
 */
public class FeedSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(FeedSink.class);
	public static final String SEARCH_TYPE = "search";
	public static final String STATISTIC_TYPE = "statistic";
	public static final String MONITOR = "monitor";

	private Path buffer;

	protected String dbHost;
	protected String dbPort;
	protected String dbUsername;
	protected String dbPassword;
	protected String dbTemplate;
	protected String format;
	protected int thrsehold;
	protected boolean isAsync;
	protected String asyncBatch;
	protected String asyncTimeout;

	protected TRSConnectionPool dbPools;
	// private Producer<String, String> producer = null;

	protected Redisson redisson;

	protected String redis;
	protected String subscribersKey;

	protected String kafka;

	protected FeedCounter feedCounter;
	protected int batchSize;
	protected Path bufferDir;
	protected Path errorDir;
	protected Path reloadDir;

	private Properties props;

	public Path selectBuffer(Event e) {
		if (buffer == null) {
			try {
				buffer = Files.createTempFile(bufferDir, getName(), ".trs");
			} catch (IOException e1) {
				LOG.error("create temp trs file failed.", e);
			}
		}
		return buffer;
	}

	protected void backup(String errorFile, Path buffer) throws IOException {
		Path errorPath = FileSystems.getDefault().getPath(errorFile);
		Files.move(errorPath,
				errorDir.resolve(String.format("%s.%s", System
						.currentTimeMillis(), errorPath.getFileName()
						.toString())), StandardCopyOption.REPLACE_EXISTING);
		Files.move(buffer, errorDir.resolve(String.format("%s.%s.%s",
				System.currentTimeMillis(), errorPath.getFileName().toString(),
				buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
	}

	protected void backup(TRSException error, Path buffer) throws IOException {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		error.printStackTrace(pw);
		pw.flush();
		sw.flush();

		Files.write(reloadDir.resolve(String.format("%s.%s",
				System.currentTimeMillis(), "ERR")), sw.toString().getBytes(),
				StandardOpenOption.CREATE);

		Files.move(buffer, reloadDir.resolve(String.format("%s.%s",
				System.currentTimeMillis(), buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
	}

	protected String load() {
		TRSConnection connection = dbPools.getTRSConnection();
		String tempDB = String.format("%s_%s", dbTemplate,
				System.currentTimeMillis());
		try {
			new TRSDataBase(connection, tempDB).create(dbTemplate + ".*");
		} catch (TRSException e) {
			LOG.error("create {} failed. ", tempDB, e);
			tempDB = null;
		}
		try {
			TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
			RecordReport report = connection.loadRecords(tempDB, dbUsername,
					buffer.toString(), null, false);

			LOG.info("{} loaded on {}. success: " + report.lSuccessNum
					+ ", failure: " + report.lFailureNum + "",
					buffer.toString(), getName());
			feedCounter.addToLoadSuccessCount(report.lSuccessNum);

			if (StringUtils.isEmpty(report.WrongFile)) {// Backup
				try {
					Files.delete(buffer);
				} catch (IOException e) {
					LOG.error("LOAD ERROR: delete trs file failed.", e);
				}
			} else {
				feedCounter.addToLoadFailureCount(report.lFailureNum);
				try {
					backup(report.WrongFile, buffer);
				} catch (IOException e) {
					LOG.error("LOAD ERROR: backup wrong file failed.", e);
				}
			}
		} catch (TRSException e) {
			try {
				backup(e, buffer);
			} catch (IOException e1) {
				LOG.error("LOAD ERROR: backup exception file failed.", e1);
			}
		} finally {
			buffer = null;
			dbPools.releaseConn(connection);
		}
		return tempDB;
	}

	private void resetCounter() {
		feedCounter.resetRedisStatist();
		feedCounter.resetTrsserverStatist();
		feedCounter.resetKafkaStatist();
		feedCounter.setCurrentFanoutCount(0);
	}

	protected void fanout(final String db) {
		resetCounter();

		final long redisBegin = System.currentTimeMillis();
		final ConcurrentMap<String, String> subscribers = redisson
				.getMap(subscribersKey);
		final String[] subscribersKeys = subscribers.keySet().toArray(
				new String[] {});
		final long redisEnd = System.currentTimeMillis();
		feedCounter.addRedisStatist(redisEnd - redisBegin);

		class FeedTask extends RecursiveAction {
			private static final long serialVersionUID = -7507406545171937726L;

			private int start;
			private int length;

			public FeedTask(int start, int length) {
				this.start = start;
				this.length = length;
			}

			public void select() {
				for (int t = start; t < length; t++) {
					String topic = subscribersKeys[t];
					String json = subscribers.get(topic);
					Map<String, ?> recordMap = null;

					final long trsserverBegin = System.currentTimeMillis();
					if (json.contains(SEARCH_TYPE)) {
						SearchFeed search = parseJSON(json, SearchFeed.class);
						recordMap = search(db, search.getTrsl(), topic);
					} else if (json.contains(STATISTIC_TYPE)) {
						StatisticFeed statistic = parseJSON(json,
								StatisticFeed.class);
						recordMap = statistic(db, statistic.getTrsl(),
								statistic.getField(), statistic.getValues(),
								statistic.isReg(), statistic.getMaxResult(),
								topic);
					}
					final long trsserverEnd = System.currentTimeMillis();
					feedCounter.addTrsserverStatist(trsserverEnd
							- trsserverBegin);

					if (recordMap != null && !recordMap.isEmpty()) {
						final long kafkaBegin = System.currentTimeMillis();
						send(topic, recordMap);
						final long kafkaEnd = System.currentTimeMillis();
						feedCounter.addKafkaStatist(kafkaEnd - kafkaBegin);
					}
					feedCounter.incrementCurrentFanoutCount();
					feedCounter.incrementFanoutCount();
				}
			}

			@Override
			protected void compute() {
				if ((length - start) < thrsehold) {
					select();
					return;
				}

				int split = (length - start) / 2;
				invokeAll(new FeedTask(start, split), new FeedTask(start
						+ split, length));

			}
		}

		FeedTask task = new FeedTask(0, subscribersKeys.length);

		final long begin = System.currentTimeMillis();
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		forkJoinPool.invoke(task);
		final long end = System.currentTimeMillis();

		feedCounter.setFanoutTime(end - begin);
		clean(db);
		feedCounter.incrementFanoutRoundCount();
	}

	protected void clean(String db) {
		if (StringHelper.isNotEmpty(db)) {
			TRSConnection connection = dbPools.getTRSConnection();
			try {
				TRSDataBase[] databases = connection.getDataBases(db);
				for (TRSDataBase database : databases) {
					database.delete();
				}
				feedCounter.incrementCleanSuccessCount();
			} catch (TRSException e) {
				feedCounter.incrementCleanFailureCount();
				LOG.error("database clean failed! ", e);
			} finally {
				dbPools.releaseConn(connection);
			}
		} else {
			LOG.error("clean empty db. ");
			feedCounter.incrementCleanFailureCount();
		}
	}

	private synchronized Map<String, String> search(String db, String trsl,
			String topic) {
		Map<String, String> record = new Hashtable<String, String>();

		TRSConnection conn = dbPools.getTRSConnection();

		TRSResultSet resultSet = null;
		try {
			resultSet = conn.executeSelect(db, trsl, false);
			for (int i = 0; i < resultSet.getRecordCount(); i++) {
				resultSet.moveTo(0, i);
				for (int cc = 0; cc < resultSet.getColumnCount(); cc++) {
					if (resultSet.getColumnName(cc).equals("IR_CONTENT")) {
						continue;
					}
					record.put(resultSet.getColumnName(cc),
							resultSet.getString(cc));
				}
			}
		} catch (TRSException e) {
			LOG.error("trs server select failed. Topic: " + topic + ", trsl: "
					+ trsl, e);
		} finally {
			if (resultSet != null && !resultSet.isClosed())
				resultSet.close();
			dbPools.releaseConn(conn);
		}
		return record;
	}

	private synchronized Map<String, Integer> statistic(String db, String trsl,
			String field, String values, boolean isReg, int max, String topic) {
		Map<String, Integer> record = new Hashtable<String, Integer>();

		TRSConnection conn = dbPools.getTRSConnection();

		TRSResultSet resultSet = null;
		try {
			resultSet = conn.executeSelect(db, trsl, false);
			int iClassNum = resultSet.classResult(field, values, 0, "", false,
					isReg ? TRSConstant.TCM_CLASSREGEXP
							| TRSConstant.TCM_OUT_BY_COUNT
							: TRSConstant.TCM_OUT_BY_COUNT);

			int to = Math.min(iClassNum, max);
			for (int i = 0; i < to; i++) {
				ClassInfo classInfo = resultSet.getClassInfo(i);
				record.put(classInfo.strValue, classInfo.iRecordNum);
			}
		} catch (TRSException e) {
			LOG.error("trs server statistic failed. Topic: " + topic
					+ ", trsl: " + trsl, e);
		} finally {
			if (resultSet != null && !resultSet.isClosed()) {
				resultSet.close();
			}
			dbPools.releaseConn(conn);
		}
		return record;
	}

	private synchronized void send(String topic, Map<?, ?> record) {
		String recordJSON = toJSON(record);

		if (StringHelper.isNotEmpty(recordJSON)) {
			try {
				Producer<String, String> producer = new Producer<String, String>(
						new ProducerConfig(props));
				producer.send(new KeyedMessage<String, String>(topic,
						recordJSON));
				incrementTopic(topic);
			} catch (Exception e) {
				LOG.error("kafka send error. ", e);
			}
		}
	}

	private String toJSON(Map<?, ?> record) {
		try {
			return new ObjectMapper().writeValueAsString(record);
		} catch (Exception e) {
			LOG.error("record to json failed. ", e);
			return null;
		}
	}

	private <T> T parseJSON(String json, Class<T> valueType) {
		try {
			return new ObjectMapper().readValue(json, valueType);
		} catch (IOException e) {
			LOG.error("parse feed error. ", e);
			return null;
		}
	}

	private void incrementTopic(String topic) {
		RAtomicLong count = redisson.getAtomicLong(MONITOR + ":" + topic);
		count.getAndIncrement();
	}

	@Override
	public synchronized void start() {
		feedCounter.start();
		super.start();
		dbPools = new TRSConnectionPool(dbHost, dbPort, dbUsername, dbPassword,
				bufferDir.toString());

		Config config = new Config();
		config.setConnectionPoolSize(10);
		config.addAddress(redis);
		redisson = Redisson.create(config);

		props = new Properties();
		props.put("metadata.broker.list", kafka);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		if (isAsync) {
			// 异步发送
			props.put("producer.type", "async");
			// 每次发送多少条
			props.put("batch.num.messages", asyncBatch);
			props.put("queue.enqueue.timeout.ms", asyncTimeout);
		}
	}

	@Override
	public synchronized void stop() {
		dbPools.destroy();
		redisson.shutdown();
		super.stop();
		feedCounter.stop();
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
		try {
			transaction.begin();
			int count = 0;
			for (count = 0; count < batchSize; count++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				TRSFileBuilder.append(selectBuffer(event), event, format);
			}

			if (count == 0) {
				status = Status.BACKOFF;
			}

			if (count > 0) {
				String tempDB = load();
				fanout(tempDB);
			}

			transaction.commit();
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
		dbHost = context.getString("dbHost");
		dbPort = context.getString("dbPort", "8888");
		dbUsername = context.getString("dbUsername", "system");
		dbPassword = context.getString("dbPassword", "manager");
		dbTemplate = context.getString("dbTemplate");
		format = context.getString("format", TRSFileBuilder.BODY_PLACEHOLDER)
				+ "\n";
		thrsehold = context.getInteger("thrsehold", 1000);
		asyncBatch = context.getString("asyncBatch", "1000");
		asyncTimeout = context.getString("asyncTimeout", "5000");
		isAsync = context.getBoolean("async", false);

		redis = context.getString("redis");
		subscribersKey = context.getString("subscribersKey");

		kafka = context.getString("kafka");

		batchSize = context.getInteger("batchSize", 1000);

		bufferDir = FileSystems.getDefault().getPath(
				context.getString("bufferDir"));
		errorDir = FileSystems.getDefault().getPath(
				context.getString("errorDir"));
		reloadDir = FileSystems.getDefault().getPath(
				context.getString("reloadDir"));

		if (feedCounter == null) {
			feedCounter = new FeedCounter(getName());
		}
	}

	static class TRSFileBuilder {

		public static final String IGNORE_HEADER_PREFIX = ".";
		public static final String BODY_PLACEHOLDER = "{body}";

		public static void append(Path path, Event e, String format)
				throws IOException {
			append(path, e, format, true);
		}

		public static void append(Path path, Event e, String format,
				boolean withEventHeaders) throws IOException {
			StringBuilder sb = new StringBuilder(format.replace(
					BODY_PLACEHOLDER, new String(e.getBody())));

			Map<String, String> headers = e.getHeaders();
			for (String key : headers.keySet()) {
				if (key.startsWith(IGNORE_HEADER_PREFIX)) {
					continue;
				}
				sb.append("<").append(key).append(">=")
						.append(headers.get(key)).append("\n");
			}

			Files.write(path, sb.toString().getBytes(),
					StandardOpenOption.APPEND);
		}
	}

}
