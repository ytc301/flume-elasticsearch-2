package com.trs.smas.flume;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class FeedCounter extends MonitoredCounterGroup implements
		FeedCounterMBean {
	private static final String COUNTER_LOAD_SUCCESS = "feed.load.success";

	private static final String COUNTER_LOAD_FAILURE = "feed.load.failure";

	private static final String COUNTER_FANOUT_SELECT_SUCCESS = "feed.fanout.select.success";

	private static final String COUNTER_FANOUT_SELECT_FAILURE = "feed.fanout.select.failure";

	private static final String COUNTER_FANOUT_CURRENT_SELECT_SUCCESS = "feed.fanout.current.select.success";

	private static final String COUNTER_FANOUT_CURRENT_SELECT_FAILURE = "feed.fanout.current.select.failure";

	private static final String COUNTER_FANOUT_ROUND = "feed.fanout.round";

	private static final String COUNTER_FANOUT = "feed.fanout";

	private static final String COUNTER_FANOUT_SEND = "feed.fanout.send";

	private static final String COUNTER_CURRENT_FANOUT = "feed.current.fanout";

	private static final String COUNTER_CURRENT_FANOUT_SEND = "feed.current.fanout.send";

	private static final String TIMER_FANOUT = "feed.fanout.time";

	private static final String COUNTER_CLEAN_SUCCESS = "feed.clean.success";

	private static final String COUNTER_CLEAN_FAILURE = "feed.clean.failure";

	private static final String[] ATTRIBUTES = { COUNTER_LOAD_SUCCESS,
			COUNTER_LOAD_FAILURE, COUNTER_FANOUT_SELECT_SUCCESS,
			COUNTER_FANOUT_SELECT_FAILURE, COUNTER_FANOUT_ROUND,
			COUNTER_FANOUT_CURRENT_SELECT_SUCCESS,
			COUNTER_FANOUT_CURRENT_SELECT_FAILURE, COUNTER_CURRENT_FANOUT,
			COUNTER_CURRENT_FANOUT_SEND, COUNTER_FANOUT, TIMER_FANOUT,
			COUNTER_FANOUT_SEND, COUNTER_CLEAN_SUCCESS, COUNTER_CLEAN_FAILURE };

	protected DescriptiveStatistics currentSelectStatistics = null;
	protected DescriptiveStatistics currentSendStatistics = null;

	protected FeedCounter(String name) {
		super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
		currentSelectStatistics = new DescriptiveStatistics();
		currentSendStatistics = new DescriptiveStatistics();
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

	public long incrementFanoutCount() {
		return increment(COUNTER_FANOUT);
	}

	public long getFanoutCount() {
		return get(COUNTER_FANOUT);
	}

	public void addFanoutSelectStatist(double value) {
		currentSelectStatistics.addValue(value);
	}

	public void resetFanoutSelectStatist() {
		currentSelectStatistics.clear();
	}

	public double getFanoutSelectMin() {
		return currentSelectStatistics.getMin();
	}

	public double getFanoutSelectMax() {
		return currentSelectStatistics.getMax();
	}

	public double getFanoutSelectMean() {
		return currentSelectStatistics.getMean();
	}

	public double getFanoutSelectMedian() {
		return currentSelectStatistics.getPercentile(50);
	}

	public double getFanoutSelectSum() {
		return currentSelectStatistics.getSum();
	}

	public double getFanoutSelect75thPercentile() {
		return currentSelectStatistics.getPercentile(75);
	}

	public double getFanoutSelect90thPercentile() {
		return currentSelectStatistics.getPercentile(90);
	}

	public double getFanoutSelect95thPercentile() {
		return currentSelectStatistics.getPercentile(95);
	}

	public double getFanoutSelect98thPercentile() {
		return currentSelectStatistics.getPercentile(98);
	}

	public double getFanoutSelect99thPercentile() {
		return currentSelectStatistics.getPercentile(99);
	}

	public void addFanoutSendStatist(double value) {
		currentSendStatistics.addValue(value);
	}

	public void resetFanoutSendStatist() {
		currentSendStatistics.clear();
	}

	public double getFanoutSendMin() {
		return currentSendStatistics.getMin();
	}

	public double getFanoutSendMax() {
		return currentSendStatistics.getMax();
	}

	public double getFanoutSendMean() {
		return currentSendStatistics.getMean();
	}

	public double getFanoutSendMedian() {
		return currentSendStatistics.getPercentile(50);
	}

	public double getFanoutSendSum() {
		return currentSendStatistics.getSum();
	}

	public double getFanoutSend75thPercentile() {
		return currentSendStatistics.getPercentile(75);
	}

	public double getFanoutSend90thPercentile() {
		return currentSendStatistics.getPercentile(90);
	}

	public double getFanoutSend95thPercentile() {
		return currentSendStatistics.getPercentile(95);
	}

	public double getFanoutSend98thPercentile() {
		return currentSendStatistics.getPercentile(98);
	}

	public double getFanoutSend99thPercentile() {
		return currentSendStatistics.getPercentile(99);
	}

	public long incrementCleanSuccessCount() {
		return increment(COUNTER_CLEAN_SUCCESS);
	}

	public long getCleanSuccessCount() {
		return get(COUNTER_CLEAN_SUCCESS);
	}

	public long incrementCleanFailureCount() {
		return increment(COUNTER_CLEAN_FAILURE);
	}

	public long getCleanFailureCount() {
		return get(COUNTER_CLEAN_FAILURE);
	}

	public long incrementFanoutRoundCount() {
		return increment(COUNTER_FANOUT_ROUND);
	}

	public long getFanoutRoundCount() {
		return get(COUNTER_FANOUT_ROUND);
	}

	public void setCurrentFanoutSelectSuccessCount(long value) {
		set(COUNTER_FANOUT_CURRENT_SELECT_SUCCESS, value);
	}

	public long incrementCurrentFanoutSelectSuccessCount() {
		return increment(COUNTER_FANOUT_CURRENT_SELECT_SUCCESS);
	}

	public long getCurrentFanoutSelectSuccessCount() {
		return get(COUNTER_FANOUT_CURRENT_SELECT_SUCCESS);
	}

	public void setCurrentFanoutSelectFailureCount(long value) {
		set(COUNTER_FANOUT_CURRENT_SELECT_FAILURE, 0);
	}

	public long incrementCurrentFanoutSelectFailureCount() {
		return increment(COUNTER_FANOUT_CURRENT_SELECT_FAILURE);
	}

	public long getCurrentFanoutSelectFailureCount() {
		return get(COUNTER_FANOUT_CURRENT_SELECT_FAILURE);
	}

	public void setCurrentFanoutSendCount(long value) {
		set(COUNTER_FANOUT_SEND, 0);
	}

	public long incrementCurrentFanoutSendCount() {
		return increment(COUNTER_FANOUT_SEND);
	}

	public long getCurrentFanoutSendCount() {
		return get(COUNTER_FANOUT_SEND);
	}

	public void setCurrentFanoutCount(long value) {
		set(COUNTER_CURRENT_FANOUT, 0);
	}

	public long incrementCurrentFanoutCount() {
		return increment(COUNTER_CURRENT_FANOUT);
	}

	public long getCurrentFanoutCount() {
		return get(COUNTER_CURRENT_FANOUT);
	}
}
