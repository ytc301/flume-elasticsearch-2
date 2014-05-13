package com.trs.smas.flume;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class FeedCounter extends MonitoredCounterGroup implements
		FeedCounterMBean {
	private static final String COUNTER_LOAD_SUCCESS = "sink.feed.load.success";

	private static final String COUNTER_LOAD_FAILURE = "sink.feed.load.failure";

	private static final String COUNTER_FANOUT_SELECT_SUCCESS = "sink.feed.fanout.select.success";

	private static final String COUNTER_FANOUT_SELECT_FAILURE = "sink.feed.fanout.select.failure";

	private static final String COUNTER_FANOUT_SEND = "sink.feed.fanout.send";

	private static final String TIMER_FANOUT = "sink.feed.fanout.time";

	private static final String[] ATTRIBUTES = { COUNTER_LOAD_SUCCESS,
			COUNTER_LOAD_FAILURE, COUNTER_FANOUT_SELECT_SUCCESS,
			COUNTER_FANOUT_SELECT_FAILURE, COUNTER_FANOUT_SEND, TIMER_FANOUT };

	protected FeedCounter(String name) {
		super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
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

	public long getFanoutSelectSuccessCount() {
		return get(COUNTER_FANOUT_SELECT_SUCCESS);
	}

	public long incrementFanoutSelectSuccessCount() {
		return increment(COUNTER_FANOUT_SELECT_SUCCESS);
	}

	public long getFanoutSelectFailureCount() {
		return get(COUNTER_FANOUT_SELECT_FAILURE);
	}

	public long incrementFanoutSelectFailureCount() {
		return increment(COUNTER_FANOUT_SELECT_FAILURE);
	}

	public void setFanoutTime(long value) {
		set(TIMER_FANOUT, value);
	}

	public long getFanoutTime() {
		return get(TIMER_FANOUT);
	}

	public long incrementFanoutSendCount() {
		return increment(COUNTER_FANOUT_SEND);
	}

	public long getFanoutSendCount() {
		return get(COUNTER_FANOUT_SEND);
	}
}
