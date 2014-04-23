/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Offset based watermark
 * @deprecated using {@link Watermark}
 * @since huangshengbo @ Apr 16, 2014 9:45:20 PM
 *
 */
public class LegacyWatermark {

    private String identifier;
    private String cursor;
    private long offset = 0L;

    public LegacyWatermark(String identifier, String cursor){
        this(identifier, cursor, 0L);
    }

    public LegacyWatermark(String identifier, String cursor, long offset){
        this.identifier = identifier;
        this.cursor = cursor;
        this.offset = offset;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
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

    public String toString(){
        return new ToStringBuilder(this)
                .append("identifier", getIdentifier())
                .append("cursor", getCursor())
                .append("offset", getOffset()).toString();
    }
}
