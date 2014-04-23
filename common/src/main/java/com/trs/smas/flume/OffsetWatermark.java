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

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * 基于偏移量的水位线,适用于cursor值相等时能保证严格升序的情况
 * @since huangshengbo @ Apr 16, 2014 9:45:20 PM
 *
 */
public class OffsetWatermark implements Serializable {

	private static final long serialVersionUID = 974415938880460887L;
	
	private String applyTo;
    private String cursor;
    private long offset = 0L;

    public OffsetWatermark(String identifier, String cursor){
        this(identifier, cursor, 0L);
    }

    public OffsetWatermark(String applyTo, String cursor, long offset){
        this.applyTo = applyTo;
        this.cursor = cursor;
        this.offset = offset;
    }

    public String getApplyTo() {
        return applyTo;
    }

    public void setApplyTo(String applyTo) {
        this.applyTo = applyTo;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void rise(String cursor){
        if(StringUtils.isEmpty(this.cursor) || this.cursor.compareTo(cursor) < 0){
            this.cursor = cursor;
            this.offset = 1;
        }else { /* if(this.cursor.equals(cursor)) 注释检查，增强容错性，如数据中该字段值为空，或不按顺序增长等等 */
            this.offset++;
        }
    }

	/** 
	 * 从文件加载watermark
	 * @param path
	 * @return 文件不存在时返回null
	 * @throws IOException
	 * @since huangshengbo @ Apr 23, 2014 11:02:14 AM
	*/
	public static OffsetWatermark loadFrom(Path path) throws IOException{
		if(!Files.exists(path)){
			return null;
		}
		return (OffsetWatermark)SerializationUtils.deserialize(Files.readAllBytes(path));
	}
	
	public void saveTo(Path path) throws IOException{
		Files.write(path, SerializationUtils.serialize(this), StandardOpenOption.CREATE);
	}
    
    public String toString(){
        return new ToStringBuilder(this)
                .append("applyTo", getApplyTo())
                .append("cursor", getCursor())
                .append("offset", getOffset()).toString();
    }
}
