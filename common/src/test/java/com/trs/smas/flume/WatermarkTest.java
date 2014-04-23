/**
 * Title:		TRS SMAS
 * Copyright:	Copyright(c) 2011-2014,TRS. All rights reserved.
 * Company:		北京拓尔思信息技术股份有限公司(www.trs.com.cn)
 */
package com.trs.smas.flume;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * TODO
 * @since huangshengbo @ Apr 23, 2014 10:46:50 AM
 *
 */
public class WatermarkTest {

	@Test
	public void test() {
		DiscreteWatermark w = new DiscreteWatermark("", "2014.04.23");
		assertTrue( w.isOverflow("2014.04.22", "abc"));
		assertFalse( w.isOverflow("2014.04.23", "abc"));
		w.rise("2014.04.23", "abc");
		assertTrue( w.isOverflow("2014.04.23", "abc"));
		assertFalse( w.isOverflow("2014.04.23", "abd"));
	}

}
