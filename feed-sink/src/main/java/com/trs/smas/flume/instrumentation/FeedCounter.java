package com.trs.smas.flume.instrumentation;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class FeedCounter extends MonitoredCounterGroup implements
		FeedCounterMBean {
	private static final String COUNTER_LOAD_SUCCESS = "feed.load.success";

	private static final String COUNTER_LOAD_FAILURE = "feed.load.failure";

	private static final String TIMER_FANOUT = "feed.fanout.time";

	private static final String COUNTER_FANOUT_ROUND = "feed.fanout.round";

	private static final String COUNTER_FANOUT = "feed.fanout";

	private static final String COUNTER_CURRENT_FANOUT = "feed.current.fanout";

	private static final String COUNTER_REDIS = "feed.redis";

	private static final String COUNTER_CURRENT_REDIS = "feed.current.redis";

	private static final String COUNTER_TRSSERVER = "feed.trsserver";

	private static final String COUNTER_CURRENT_TRSSERVER = "feed.current.trsserver";

	private static final String COUNTER_KAFKA = "feed.kafka";

	private static final String COUNTER_CURRENT_KAFKA = "feed.current.kafka";

	private static final String COUNTER_CLEAN_SUCCESS = "feed.clean.success";

	private static final String COUNTER_CLEAN_FAILURE = "feed.clean.failure";

	private static final String[] ATTRIBUTES = { COUNTER_LOAD_SUCCESS,
			COUNTER_LOAD_FAILURE, COUNTER_FANOUT_ROUND, TIMER_FANOUT,
			COUNTER_FANOUT, COUNTER_CURRENT_FANOUT, COUNTER_REDIS,
			COUNTER_CURRENT_REDIS, COUNTER_TRSSERVER,
			COUNTER_CURRENT_TRSSERVER, COUNTER_KAFKA, COUNTER_CURRENT_KAFKA,
			COUNTER_CLEAN_SUCCESS, COUNTER_CLEAN_FAILURE };

	protected DescriptiveStatistics currentRedisStatistics = null;
	protected DescriptiveStatistics currentTrsserverStatistics = null;
	protected DescriptiveStatistics currentKafkaStatistics = null;

	public FeedCounter(String name) {
		super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
		currentRedisStatistics = new DescriptiveStatistics();
		currentTrsserverStatistics = new DescriptiveStatistics();
		currentKafkaStatistics = new DescriptiveStatistics();
	}

	public void resetRedisStatist() {
		if (currentRedisStatistics != null) {
			currentRedisStatistics.clear();
		}
	}

	public void resetTrsserverStatist() {
		if (currentTrsserverStatistics != null) {
			currentTrsserverStatistics.clear();
		}
	}

	public void resetKafkaStatist() {
		if (currentKafkaStatistics != null) {
			currentKafkaStatistics.clear();
		}
	}

	public long getLoadSuccessCount() {
		return get(COUNTER_LOAD_SUCCESS);
	}

	public long addToLoadSuccessCount(long delta) {
		return addAndGet(COUNTER_LOAD_SUCCESS, delta);
	}

	public long getLoadFailureCount() {
		return get(COUNTER_LOAD_FAILURE);
	}

	public long addToLoadFailureCount(long delta) {
		return addAndGet(COUNTER_LOAD_FAILURE, delta);
	}

	public long getFanoutTime() {
		return get(TIMER_FANOUT);
	}

	public void setFanoutTime(long value) {
		set(TIMER_FANOUT, value);
	}

	public long getFanoutCount() {
		return get(COUNTER_FANOUT);
	}

	public long incrementFanoutCount() {
		return increment(COUNTER_FANOUT);
	}

	public long getFanoutRoundCount() {
		return get(COUNTER_FANOUT_ROUND);
	}

	public long incrementFanoutRoundCount() {
		return increment(COUNTER_FANOUT_ROUND);
	}

	public long getCurrentFanoutCount() {
		return get(COUNTER_CURRENT_FANOUT);
	}

	public long incrementCurrentFanoutCount() {
		return increment(COUNTER_CURRENT_FANOUT);
	}

	public void setCurrentFanoutCount(long value) {
		set(COUNTER_CURRENT_FANOUT, 0);
	}

	public void addRedisStatist(double value) {
		currentRedisStatistics.addValue(value);
	}

	public double getRedisMin() {
		return currentRedisStatistics.getMin();
	}

	public double getRedisMax() {
		return currentRedisStatistics.getMax();
	}

	public double getRedisMean() {
		return currentRedisStatistics.getMean();
	}

	public double getRedisMedian() {
		return currentRedisStatistics.getPercentile(50);
	}

	public double getRedisSum() {
		return currentRedisStatistics.getSum();
	}

	public double getRedis75thPercentile() {
		return currentRedisStatistics.getPercentile(75);
	}

	public double getRedis90thPercentile() {
		return currentRedisStatistics.getPercentile(90);
	}

	public double getRedis95thPercentile() {
		return currentRedisStatistics.getPercentile(95);
	}

	public double getRedis98thPercentile() {
		return currentRedisStatistics.getPercentile(98);
	}

	public double getRedis99thPercentile() {
		return currentRedisStatistics.getPercentile(99);
	}

	public double getRedis999thPercentile() {
		return currentRedisStatistics.getPercentile(99.9);
	}

	public void addTrsserverStatist(double value) {
		currentTrsserverStatistics.addValue(value);
	}

	public double getTrsserverMin() {
		return currentTrsserverStatistics.getMin();
	}

	public double getTrsserverMax() {
		return currentTrsserverStatistics.getMax();
	}

	public double getTrsserverMean() {
		return currentTrsserverStatistics.getMean();
	}

	public double getTrsserverMedian() {
		return currentTrsserverStatistics.getPercentile(50);
	}

	public double getTrsserverSum() {
		return currentTrsserverStatistics.getSum();
	}

	public double getTrsserver75thPercentile() {
		return currentTrsserverStatistics.getPercentile(75);
	}

	public double getTrsserver90thPercentile() {
		return currentTrsserverStatistics.getPercentile(90);
	}

	public double getTrsserver95thPercentile() {
		return currentTrsserverStatistics.getPercentile(95);
	}

	public double getTrsserver98thPercentile() {
		return currentTrsserverStatistics.getPercentile(98);
	}

	public double getTrsserver99thPercentile() {
		return currentTrsserverStatistics.getPercentile(99);
	}

	public double getTrsserver999thPercentile() {
		return currentTrsserverStatistics.getPercentile(99.9);
	}

	public void addKafkaStatist(double value) {
		currentKafkaStatistics.addValue(value);
	}

	public double getKafkaMin() {
		return currentKafkaStatistics.getMin();
	}

	public double getKafkaMax() {
		return currentKafkaStatistics.getMax();
	}

	public double getKafkaMean() {
		return currentKafkaStatistics.getMean();
	}

	public double getKafkaMedian() {
		return currentKafkaStatistics.getPercentile(50);
	}

	public double getKafkaSum() {
		return currentKafkaStatistics.getSum();
	}

	public double getKafka75thPercentile() {
		return currentKafkaStatistics.getPercentile(75);
	}

	public double getKafka90thPercentile() {
		return currentKafkaStatistics.getPercentile(90);
	}

	public double getKafka95thPercentile() {
		return currentKafkaStatistics.getPercentile(95);
	}

	public double getKafka98thPercentile() {
		return currentKafkaStatistics.getPercentile(98);
	}

	public double getKafka99thPercentile() {
		return currentKafkaStatistics.getPercentile(99);
	}

	public double getKafka999thPercentile() {
		return currentKafkaStatistics.getPercentile(99.9);
	}

	public long getCleanSuccessCount() {
		return get(COUNTER_CLEAN_SUCCESS);
	}

	public long incrementCleanSuccessCount() {
		return increment(COUNTER_CLEAN_SUCCESS);
	}

	public long getCleanFailureCount() {
		return get(COUNTER_CLEAN_FAILURE);
	}

	public long incrementCleanFailureCount() {
		return increment(COUNTER_CLEAN_FAILURE);
	}
}
