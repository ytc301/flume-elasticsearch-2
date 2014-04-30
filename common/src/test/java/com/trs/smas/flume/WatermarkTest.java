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
	
	@Test
	public void testNGram() {
		NGramOffsetWatermark w = new NGramOffsetWatermark("", "2014.04.23",3);
		w.rise("2014.04.23");
		assertEquals(1,w.getOffset());
		assertEquals("2014.04.23",w.getCursor());
		
		w.rise("2014.04.23");
		assertEquals(2,w.getOffset());
		assertEquals("2014.04.23",w.getCursor());
		
		w.rise("2014.04.24");
		assertEquals(3,w.getOffset());
		assertEquals("2014.04.23",w.getCursor());
		
		w.rise("2014.04.25");
		assertEquals(4,w.getOffset());
		assertEquals("2014.04.23",w.getCursor());
		
		w.rise("2014.04.26");
		assertEquals(3,w.getOffset());
		assertEquals("2014.04.24",w.getCursor());
	}

}
