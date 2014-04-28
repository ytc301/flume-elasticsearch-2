/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * N元偏移量的水位线, 记录最新n个cursor的偏移量
 * 
 * @since huangshengbo @ Apr 28, 2014 4:08:58 PM
 * 
 */
public class NGramOffsetWatermark implements Serializable {

	private static final long serialVersionUID = 3605181463710435597L;

	private String applyTo;
	private Multiset<String> cursors;
	private int n;

	public NGramOffsetWatermark(String applyTo, String cursor, int n) {
		this.applyTo = applyTo;
		this.n = n;
		cursors = HashMultiset.create(n);
	}

	public String getApplyTo() {
		return applyTo;
	}

	public void setApplyTo(String applyTo) {
		this.applyTo = applyTo;
	}

	public String getCursor() {
		return Collections.min(cursors.elementSet());
	}

	public long getOffset() {
		return cursors.size();
	}

	public void rise(String cursor) {
		if (getCursor().compareTo(cursor) > 0) {
			throw new IllegalArgumentException("We just cann't back to "+cursor+", current: "+ getCursor());
		}

		cursors.add(cursor);
		if (cursors.elementSet().size() > n) {
			cursors.remove(getCursor(), Integer.MAX_VALUE);
		}
	}

	/**
	 * 从文件加载watermark
	 * 
	 * @param path
	 * @return 文件不存在时返回null
	 * @throws IOException
	 * @since huangshengbo @ Apr 23, 2014 11:02:14 AM
	 */
	public static NGramOffsetWatermark loadFrom(Path path) throws IOException {
		if (!Files.exists(path)) {
			return null;
		}
		return (NGramOffsetWatermark) SerializationUtils.deserialize(Files
				.readAllBytes(path));
	}

	public void saveTo(Path path) throws IOException {
		Files.write(path, SerializationUtils.serialize(this),
				StandardOpenOption.CREATE);
	}

	public String toString() {
		return new ToStringBuilder(this).append("applyTo", getApplyTo())
				.append("cursor", getCursor()).append("offset", getOffset())
				.toString();
	}

}
