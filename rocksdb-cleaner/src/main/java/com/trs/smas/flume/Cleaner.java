package com.trs.smas.flume;

import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.trs.dev4.jdk16.utils.CollectionUtil;
import com.trs.dev4.jdk16.utils.DateUtil;

public class Cleaner {
	static {
		RocksDB.loadLibrary();
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("please check input!");
			return;
		}

		String db_path = args[0];
		int ttl = Integer.parseInt(args[1]);

		RocksDB db = null;

		Options options = new Options().setCreateIfMissing(true);
		try {
			db = RocksDB.open(options, db_path);
		} catch (RocksDBException e) {
			System.out.println("init rocksdb error: " + e);
		}

		System.out.println("rocksdb remove old record begin ...");
		long total = 0;
		RocksIterator iterator = db.newIterator();
		try {
			for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
				iterator.status();

				if (DateUtil
						.parseDate(
								new String(iterator.value(), "UTF-8"),
								CollectionUtil
										.toCollection(new String[] { DateUtil.FMT_TRS_yMd }))
						.before(DateUtils.addDays(new Date(), ttl))) {
					db.remove(iterator.key());
					total++;
				}
			}
		} catch (RocksDBException e) {
			System.out.println("rocksdb remove error: " + e);
		} catch (UnsupportedEncodingException e) {
			System.out.println("get bytes encoding error: " + e);
		}
		System.out.println("rocksdb remove old record end! Total=" + total);

		if (db != null)
			db.close();
		if (options != null)
			options.dispose();
	}
}
