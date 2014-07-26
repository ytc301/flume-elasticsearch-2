/**
 * Title:		flume-elasticsearch
 */
package com.trs.smas.flume;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * 离散水位线,用于导数据时标记文件位置和文件start positon
 * 
 * @since selamat
 * 
 */
public class DiscreteWatermark implements Serializable {

	private static final long serialVersionUID = 2905794392669383111L;

	private String applyTo;
	private String cursor;
	private BloomFilter<CharSequence> overflowedIds;
	private long offset = 0L;// 无业务价值,只是用于调试监控

	public DiscreteWatermark(String applyTo, String cursor) {
		this.applyTo = applyTo;
		this.cursor = cursor;
		this.overflowedIds = BloomFilter.create(
				Funnels.stringFunnel(Charset.forName("UTF-8")), 10000, 0.0000001);
	}

	public String getApplyTo() {
		return this.applyTo;
	}

	public String getCursor() {
		return this.cursor;
	}

	public void rise(String mark, String id) {
		if (StringUtils.isEmpty(this.cursor) || this.cursor.compareTo(mark) < 0) {
			this.cursor = mark;
			this.overflowedIds = BloomFilter.create(
					Funnels.stringFunnel(Charset.forName("UTF-8")), 10000,
					0.0000001);
			this.overflowedIds.put(id);
			this.offset = 1;
		} else {
			overflowedIds.put(id);
			this.offset++;
		}
	}

	public boolean isOverflow(String mark, String id) {
		// TODO comparison > 0 时是否要warn
		int comparison = this.cursor.compareTo(mark);
		return (comparison > 0)
				|| ((comparison == 0) && this.overflowedIds.mightContain(id));
	}

	/** 
	 * 从文件加载watermark
	 * @param path
	 * @return 文件不存在时返回null
	 * @throws IOException
	 * @since huangshengbo @ Apr 23, 2014 11:02:14 AM
	*/
	public static DiscreteWatermark loadFrom(Path path) throws IOException{
		if(!Files.exists(path)){
			return null;
		}
		return (DiscreteWatermark)SerializationUtils.deserialize(Files.readAllBytes(path));
	}
	
	public void saveTo(Path path) throws IOException{
		Files.write(path, SerializationUtils.serialize(this), StandardOpenOption.CREATE);
	}
	
	public String toString() {
		return new ToStringBuilder(this).append("applyTo", getApplyTo())
				.append("cursor", getCursor()).append("offset", offset)
				.toString();
	}
}
