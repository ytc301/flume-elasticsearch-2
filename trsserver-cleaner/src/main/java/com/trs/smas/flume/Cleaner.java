package com.trs.smas.flume;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;

public class Cleaner {

	/**
	 * @param args
	 *            args[0] = tables.conf args[1] = dbs.conf args[2] = filter
	 */
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out
					.println("please check input! e.g. java -jar Cleaner.jar /path/tables.conf /path/dbs.conf filter");
		}

		List<String> tables = new ArrayList<String>();
		List<String> dbs = new ArrayList<String>();

		try {
			tables = Files.readAllLines(
					FileSystems.getDefault().getPath(args[0]),
					StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dbs = Files.readAllLines(FileSystems.getDefault().getPath(args[1]),
					StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String table : tables) {
			if (!table.trim().isEmpty()) {
				for (String db : dbs) {
					String[] propes = db.trim().split("\t");
					if (!db.trim().isEmpty() && propes.length == 4) {
						clear(propes[0], propes[1], propes[2], propes[3],
								table, args[2].trim());
					}
				}
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
