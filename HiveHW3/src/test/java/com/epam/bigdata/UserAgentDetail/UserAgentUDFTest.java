/**
 * Test cases for the parsing the userAgent.
 */
package com.epam.bigdata.UserAgentDetail;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.bitwalker.useragentutils.UserAgent;

/**
 * @author Shiva_Donkena Test cases for the parsing the userAgent.
 */
public class UserAgentUDFTest {
	@Test
	public void testUserAgent() {
		String userAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16";
		Object[] object = UserAgentUDF.createParseDataObject(UserAgent.parseUserAgentString(userAgent));
		assertEquals("Computer", object[0]);
		assertEquals("Windows 7", object[1]);
		assertEquals("Firefox 3", object[2]);
		assertEquals("Browser", object[3]);
	}

	@Test
	public void testUserAgent1() {
		String userAgent = "Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10";
		Object[] object = UserAgentUDF.createParseDataObject(UserAgent.parseUserAgentString(userAgent));
		assertEquals("Computer", object[0]);
		assertEquals("Windows XP", object[1]);
		assertEquals("Opera 11", object[2]);
		assertEquals("Browser", object[3]);
	}
}
