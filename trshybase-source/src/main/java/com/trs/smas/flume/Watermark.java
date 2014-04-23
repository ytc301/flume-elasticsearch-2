/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * TODO
 * 
 * @since huangshengbo @ Apr 22, 2014 11:00:24 PM
 * 
 */
public class Watermark implements Serializable{

	private static final long serialVersionUID = 2905794392669383111L;
	
	private String applyTo;
	private String cursor;
	private BloomFilter<CharSequence> overflowedIds;
	private long offset = 0L;// 无业务价值,只是用于调试监控

	public Watermark(String applyTo, String cursor) {
		this.applyTo = applyTo;
		this.cursor = cursor;
		this.overflowedIds = BloomFilter.create(
				Funnels.stringFunnel(Charset.forName("UTF-8")), 5000,
				0.0003);
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
					Funnels.stringFunnel(Charset.forName("UTF-8")), 5000,
					0.0002);
			this.overflowedIds.put(id);
			this.offset = 1;
		} else {
			overflowedIds.put(id);
			this.offset ++ ;
		}
	}

	public boolean isOverflow(String mark, String id) {
		// TODO comparison > 0 时是否要warn
		int comparison = this.cursor.compareTo(mark);
		return (comparison > 0)
				|| ((comparison == 0) && this.overflowedIds.mightContain(id));
	}
	
    public String toString(){
        return new ToStringBuilder(this)
                .append("applyTo", getApplyTo())
                .append("cursor", getCursor())
                .append("offset", offset).toString();
    }
}
