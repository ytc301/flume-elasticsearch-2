package com.trs.smas.flume.type;

public class StatisticFeed extends Feed {
	private String field;
	private String values;
	private boolean isReg;
	private int maxResult;

	public StatisticFeed() {
		super();
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getValues() {
		return values;
	}

	public void setValues(String values) {
		this.values = values;
	}

	public boolean isReg() {
		return isReg;
	}

	public void setReg(boolean isReg) {
		this.isReg = isReg;
	}

	public int getMaxResult() {
		return maxResult;
	}

	public void setMaxResult(int maxResult) {
		this.maxResult = maxResult;
	}

}
