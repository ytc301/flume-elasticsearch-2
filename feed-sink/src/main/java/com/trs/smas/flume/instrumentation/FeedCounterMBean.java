package com.trs.smas.flume.instrumentation;

public interface FeedCounterMBean {
	long getLoadSuccessCount();

	long getLoadFailureCount();

	long getFanoutTime();

	long getFanoutCount();

	long getFanoutRoundCount();

	long getCurrentFanoutCount();

	double getRedisMin();

	double getRedisMax();

	double getRedisMean();

	double getRedisMedian();

	double getRedisSum();

	double getRedis75thPercentile();

	double getRedis90thPercentile();

	double getRedis95thPercentile();

	double getRedis98thPercentile();

	double getRedis99thPercentile();

	double getRedis999thPercentile();

	double getTrsserverMin();

	double getTrsserverMax();

	double getTrsserverMean();

	double getTrsserverMedian();

	double getTrsserverSum();

	double getTrsserver75thPercentile();

	double getTrsserver90thPercentile();

	double getTrsserver95thPercentile();

	double getTrsserver98thPercentile();

	double getTrsserver99thPercentile();

	double getTrsserver999thPercentile();

	double getKafkaMin();

	double getKafkaMax();

	double getKafkaMean();

	double getKafkaMedian();

	double getKafkaSum();

	double getKafka75thPercentile();

	double getKafka90thPercentile();

	double getKafka95thPercentile();

	double getKafka98thPercentile();

	double getKafka99thPercentile();

	double getKafka999thPercentile();

	long getCleanSuccessCount();

	long getCleanFailureCount();

	long getStartTime();

	long getStopTime();
}
