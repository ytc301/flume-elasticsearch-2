package com.trs.smas.flume.db.impl;

import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;
import com.trs.smas.flume.db.ITRSConnectionPool;

public class TRSConnectionPool implements ITRSConnectionPool {
	private static final Logger LOG = LoggerFactory
			.getLogger(TRSConnectionPool.class);

	private boolean isActive = false; // 连接池活动状态
	private int contActive = 0;// 记录创建的总的连接数
	private int minConnections = 1; // 空闲池，最小连接数
	private int maxConnections = 40; // 空闲池，最大连接数
	private int initConnections = 20;// 初始化连接数
	private int connTimeOut = 2000;

	private String dbHost;
	private String dbPort;
	private String dbUsername;
	private String dbPassword;
	private String bufferDir;

	// 空闲连接
	private List<TRSConnection> freeConnection = new Vector<TRSConnection>();
	// 活动连接
	private List<TRSConnection> activeConnection = new Vector<TRSConnection>();

	public TRSConnectionPool() {

	}

	public TRSConnectionPool(String dbHost, String dbPort, String dbUsername,
			String dbPassword, String bufferDir) {
		this.dbHost = dbHost;
		this.dbPort = dbPort;
		this.dbUsername = dbUsername;
		this.dbPassword = dbPassword;
		this.bufferDir = bufferDir;
		init();
	}

	public void init() {
		for (int i = 0; i < Math.max(this.minConnections,
				Math.min(this.initConnections, this.maxConnections)); i++) {
			TRSConnection connection = null;
			try {
				connection = new TRSConnection();
				connection.connect(dbHost, dbPort, dbUsername, dbPassword);
				connection.setBufferPath(bufferDir);
			} catch (TRSException e) {
				LOG.error("Unable to create connection to trsserver", e);
			}
			if (isValid(connection)) {
				freeConnection.add(connection);
				contActive++;
			}
		}
	}

	private boolean isValid(TRSConnection conn) {
		if (conn == null || conn.isClosed()) {
			return false;
		}
		return true;
	}

	public synchronized TRSConnection getTRSConnection() {
		TRSConnection conn = null;
		try {
			// 判断是否超过最大连接数限制
			if (contActive < this.maxConnections) {
				if (freeConnection.size() > 0) {
					conn = freeConnection.get(0);
					freeConnection.remove(0);
				} else {
					conn = new TRSConnection();
					conn.connect(dbHost, dbPort, dbUsername, dbPassword);
					conn.setBufferPath(bufferDir);
					contActive++;
				}
			} else {
				wait(this.connTimeOut);
				conn = getTRSConnection();
			}
			if (!isValid(conn)) {
				conn = new TRSConnection();
				conn.connect(dbHost, dbPort, dbUsername, dbPassword);
				conn.setBufferPath(bufferDir);
			}
			activeConnection.add(conn);
		} catch (InterruptedException e) {
			LOG.error("thread wait error! ", e);
		} catch (TRSException e) {
			LOG.error("get trs connection error!", e);
		}
		return conn;
	}

	public void destroy() {
		for (TRSConnection conn : freeConnection) {
			if (isValid(conn)) {
				conn.close();
			}
		}
		for (TRSConnection conn : activeConnection) {
			if (isValid(conn)) {
				conn.close();
			}
		}
		isActive = false;
		contActive = 0;
	}

	public boolean isActive() {
		return isActive;
	}

	public void releaseConn(TRSConnection conn) {
		if (isValid(conn)) {
			freeConnection.add(conn);
			activeConnection.remove(conn);
		}
	}

	public void setMinConnections(int minConnections) {
		this.minConnections = minConnections;
	}

	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}

	public void setInitConnections(int initConnections) {
		this.initConnections = initConnections;
	}

}
