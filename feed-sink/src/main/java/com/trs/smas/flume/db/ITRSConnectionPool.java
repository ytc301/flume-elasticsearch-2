package com.trs.smas.flume.db;

import com.trs.client.TRSConnection;

public interface ITRSConnectionPool {
	public TRSConnection getTRSConnection();
	
	public void releaseConn(TRSConnection conn);

	public void destroy();

	public boolean isActive();
}
