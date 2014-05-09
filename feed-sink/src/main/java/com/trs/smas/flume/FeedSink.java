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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

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
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.codehaus.jackson.map.ObjectMapper;
import org.redisson.Config;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trs.client.RecordReport;
import com.trs.client.TRSConnection;
import com.trs.client.TRSConstant;
import com.trs.client.TRSDataBase;
import com.trs.client.TRSException;
import com.trs.client.TRSResultSet;

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
 * .redisHost = 127.0.0.1<br/>
 * .redisPort = 6379<br/>
 * .subscribersKey = subscribers<br/>
 * .kafkaHost = 127.0.0.1<br/>
 * .kafkaPort = 9092
 * </code>
 * 
 * @since huangshengbo @ Apr 23, 2014 6:58:40 PM
 * 
 */
public class FeedSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(FeedSink.class);

	private Path buffer;

	protected String dbHost;
	protected String dbPort;
	protected String dbUsername;
	protected String dbPassword;
	protected String dbTemplate;
	protected String format;

	protected TRSConnection connection;

	protected Redisson redisson;

	protected String redisHost;
	protected String redisPort;
	protected String subscribersKey;

	protected String kafkaHost;
	protected String kafkaPort;

	protected SinkCounter sinkCounter;
	protected int batchSize;
	protected Path bufferDir;
	protected Path backupDir;

	private String tempDB;
	private Properties props;

	@Override
	public synchronized void start() {
		sinkCounter.start();
		super.start();
		try {
			connection = new TRSConnection();
			connection.connect(dbHost, dbPort, dbUsername, dbPassword);

			connection.setBufferPath(backupDir.toString());
		} catch (TRSException e) {
			throw new RuntimeException(
					"Unable to create connection to trsserver", e);
		}

		Config config = new Config();
		config.setConnectionPoolSize(10);
		config.addAddress(redisHost + ":" + redisPort);
		redisson = Redisson.create(config);

		props = new Properties();
		props.put("metadata.broker.list", kafkaHost + ":" + kafkaPort);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
	}

	@Override
	public synchronized void stop() {
		connection.close();
		redisson.shutdown();
		super.stop();
		sinkCounter.stop();
	}

	public Path selectBuffer(Event e) throws IOException {
		if (buffer == null) {
			buffer = Files.createTempFile(bufferDir, getName(), ".trs");
		}
		return buffer;
	}

	protected void backup(String errorFile, Path buffer) throws IOException {
		Path errorPath = FileSystems.getDefault().getPath(errorFile);
		Files.move(errorPath,
				backupDir.resolve(String.format("%s.%s", System
						.currentTimeMillis(), errorPath.getFileName()
						.toString())), StandardCopyOption.REPLACE_EXISTING);
		Files.move(buffer, backupDir.resolve(String.format("%s.%s.%s",
				System.currentTimeMillis(), errorPath.getFileName().toString(),
				buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
	}

	protected void backup(TRSException error, Path buffer) throws IOException {
		Path errorFile = FileSystems.getDefault().getPath(
				error.getErrorString());
		if (Files.exists(errorFile)) {
			Files.move(errorFile, backupDir.resolve(String.format("%s.%s",
					System.currentTimeMillis(), errorFile.getFileName()
							.toString())), StandardCopyOption.REPLACE_EXISTING);
		} else {
			Files.write(backupDir.resolve(String.format("%s.%s",
					System.currentTimeMillis(), "ERR")), error.getErrorString()
					.getBytes(), StandardOpenOption.CREATE);
		}
		Files.move(buffer, backupDir.resolve(String.format("%s.%s",
				System.currentTimeMillis(), buffer.getFileName().toString())),
				StandardCopyOption.REPLACE_EXISTING);
	}

	protected void load() throws IOException {
		tempDB = String.format("%s_%s", dbTemplate, System.currentTimeMillis());
		try {
			new TRSDataBase(connection, tempDB).create(dbTemplate + ".*");
		} catch (TRSException e) {
			LOG.error("create {} failed. ", tempDB, e);
		}
		try {
			TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
			RecordReport report = connection.loadRecords(tempDB, dbUsername,
					buffer.toString(), null, false);

			LOG.info("{} loaded on {}. success: " + report.lSuccessNum
					+ ", failure: " + report.lFailureNum + "",
					buffer.toString(), getName());
			sinkCounter.addToEventDrainSuccessCount(report.lSuccessNum);

			if (StringUtils.isEmpty(report.WrongFile)) {// Backup
				Files.delete(buffer);
			} else {
				backup(report.WrongFile, buffer);
			}
		} catch (TRSException e) {
			backup(e, buffer);
		} finally {
			buffer = null;
		}
	}

	protected void fanout() {
		final Producer<String, String> producer = new Producer<String, String>(
				new ProducerConfig(props));

		ConcurrentMap<String, String> subscribers = redisson
				.getMap(subscribersKey);
		for (String topic : subscribers.keySet()) {
			LOG.info("The subscribers topic {}, trsl {}", topic,
					subscribers.get(topic));
			TRSResultSet resultSet = null;
			try {
				resultSet = connection.executeSelect(tempDB,
						subscribers.get(topic), false);
			} catch (TRSException e) {
				LOG.error(
						"fail to select " + tempDB + " by "
								+ subscribers.get(topic), e);
				continue;
			}
			LOG.info("select {} by {}, resultset count {}", tempDB,
					subscribers.get(topic), resultSet.getRecordCount());
			try {
				for (int i = 0; i < resultSet.getRecordCount(); i++) {
					resultSet.moveTo(0, i);

					Map<String, String> record = new HashMap<String, String>();
					for (int cc = 0; cc < resultSet.getColumnCount(); cc++) {
						record.put(resultSet.getColumnName(cc),
								resultSet.getString(cc));
					}

					String recordJSON = null;

					try {
						recordJSON = new ObjectMapper()
								.writeValueAsString(record);
					} catch (Exception e) {
						LOG.error("record to json failed. ", e);
					}

					KeyedMessage<String, String> message = new KeyedMessage<String, String>(
							topic, recordJSON);

					producer.send(message);
					LOG.info("producer send topic {}", topic);
				}
			} catch (TRSException e) {
				LOG.error("can not read data from resultset " + resultSet, e);
				continue;
			} finally {
				resultSet.close();
			}
		}
	}

	protected void clean() {
		try {
			TRSDataBase[] databases = connection.getDataBases(tempDB);
			for (TRSDataBase database : databases) {
				database.delete();
			}
		} catch (TRSException e) {
			LOG.error("database clean failed! ", e);
		}
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
				sinkCounter.incrementEventDrainAttemptCount();
			}

			if (count == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else if (count < batchSize) {
				sinkCounter.incrementBatchUnderflowCount();
			} else {
				sinkCounter.incrementBatchCompleteCount();
			}

			if (count > 0) {
				load();
				fanout();
				clean();
			}

			sinkCounter.addToEventDrainSuccessCount(count);
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
		redisHost = context.getString("redisHost");
		redisPort = context.getString("redisPort", "6379");
		subscribersKey = context.getString("subscribersKey");

		kafkaHost = context.getString("kafkaHost");
		kafkaPort = context.getString("kafkaPort");

		batchSize = context.getInteger("batchSize", 1000);

		bufferDir = FileSystems.getDefault().getPath(
				context.getString("bufferDir"));
		backupDir = FileSystems.getDefault().getPath(
				context.getString("backupDir"));

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
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
