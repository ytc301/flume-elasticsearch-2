package com.trs.smas.flume;

import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;
import com.trs.dev4.jdk16.utils.DateUtil;

public class Cleaner {

	/**
	 * @param args
	 *            system manager2013admin 8899 hotspot
	 *            192.168.200.2；……；192.168，200.9 IR_GW_LOADTIME -3
	 */
	public static void main(String[] args) {
		if (args.length != 8) {
			System.out.println("please check input!");
			return;
		}

		String username = args[0];
		String password = args[1];
		String port = args[2];
		String[] hosts = args[3].split(";");
		String[] dbs = args[4].split(";");
		String filter = args[5];
		String format = args[6];
		int days = Integer.parseInt(args[7]);

		String query = filter
				+ " <= "
				+ DateUtil.date2String(DateUtils.addDays(new Date(), days),
						format);

		System.out.println(query);

		for (String host : hosts) {
			for (String db : dbs) {
				clear(host, port, username, password, db, query);
			}
		}
	}

	public static void clear(String host, String port, String username,
			String password, String database, String filter) {
		TRSConnection conn = null;
		try {
			conn = new TRSConnection();
			conn.connect(host, port, username, password, "T10");

			int iDel = conn.executeDelete(database, username, filter, false);
			System.out.println(host + "'s " + database + " has deleted " + iDel
					+ " Records!");
		} catch (TRSException e) {
			System.out.println("ErrorCode: " + e.getErrorCode());
			System.out.println("ErrorString: " + e.getErrorString());
		} finally {
			if (conn != null)
				conn.close();
			conn = null;
		}
	}
}
