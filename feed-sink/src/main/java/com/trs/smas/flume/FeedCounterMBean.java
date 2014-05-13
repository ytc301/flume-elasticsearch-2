package com.trs.smas.flume;

public interface FeedCounterMBean {
	long getLoadSuccessCount();

	long getLoadFailureCount();

	long getFanoutSelectSuccessCount();

	long getFanoutSelectFailureCount();
	
	long getFanoutSendCount();

	long getFanoutTime();

	long getStartTime();

	long getStopTime();
}
