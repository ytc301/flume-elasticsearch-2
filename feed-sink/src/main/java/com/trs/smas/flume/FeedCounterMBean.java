package com.trs.smas.flume;

public interface FeedCounterMBean {
	long getLoadSuccessCount();

	long getLoadFailureCount();

	long getFanoutSelectSuccessCount();

	long getFanoutSelectFailureCount();

	long getFanoutSendCount();

	long getFanoutCount();

	long getCurrentFanoutSelectSuccessCount();

	long getCurrentFanoutSelectFailureCount();

	long getCurrentFanoutSendCount();

	long getCurrentFanoutCount();

	long getFanoutTime();

	double getFanoutSelectMin();

	double getFanoutSelectMax();

	double getFanoutSelectMean();

	double getFanoutSelectMedian();

	double getFanoutSelectSum();

	double getFanoutSelect75thPercentile();

	double getFanoutSelect90thPercentile();

	double getFanoutSelect95thPercentile();

	double getFanoutSelect98thPercentile();

	double getFanoutSelect99thPercentile();

	double getFanoutSendMin();

	double getFanoutSendMax();

	double getFanoutSendMean();

	double getFanoutSendMedian();

	double getFanoutSendSum();

	double getFanoutSend75thPercentile();

	double getFanoutSend90thPercentile();

	double getFanoutSend95thPercentile();

	double getFanoutSend98thPercentile();

	double getFanoutSend99thPercentile();

	long getCleanSuccessCount();

	long getCleanFailureCount();

	long getFanoutRoundCount();

	long getStartTime();

	long getStopTime();
}
