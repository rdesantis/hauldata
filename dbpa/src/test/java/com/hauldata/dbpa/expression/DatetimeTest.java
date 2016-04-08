/*
 * Copyright (c) 2016, Ronald DeSantis
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */

package com.hauldata.dbpa.expression;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import junit.framework.TestCase;

public class DatetimeTest extends TestCase {

	public DatetimeTest(String name) {
		super(name);
	}

	static final IntegerConstant negativeOne = new IntegerConstant(-1);
	static final IntegerConstant two = new IntegerConstant(2);
	static final IntegerConstant three = new IntegerConstant(3);
	static final IntegerConstant negativeFour = new IntegerConstant(-4);
	static final IntegerConstant six = new IntegerConstant(6);
	static final IntegerConstant twelve = new IntegerConstant(12);
	static final IntegerConstant fifteen = new IntegerConstant(15);
	static final IntegerConstant fortyFive = new IntegerConstant(45); 
	static final IntegerConstant twentySixteen = new IntegerConstant(2016); 
	static final IntegerConstant nullInteger = new IntegerConstant(null);

	public void testDateFromParts() {

		DateFromParts fromParts;

		fromParts = new DateFromParts(twentySixteen, six, fifteen);
		assertEquals(LocalDate.of(2016, 6, 15), fromParts.evaluate().toLocalDate());

		fromParts = new DateFromParts(nullInteger, six, fifteen);
		assertNull(fromParts.evaluate());
		fromParts = new DateFromParts(twentySixteen, nullInteger, fifteen);
		assertNull(fromParts.evaluate());
		fromParts = new DateFromParts(twentySixteen, six, nullInteger);
		assertNull(fromParts.evaluate());
	}

	public void testAdd() {

		final DatetimeConstant nineteenNinetyNine = new DatetimeConstant(LocalDateTime.of(1999, 12, 31, 23, 59, 59));

		DatetimeAdd addition;

		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.YEARS, two);
		assertEquals(LocalDateTime.of(2001, 12, 31, 23, 59, 59), addition.evaluate());
		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.MONTHS, negativeFour);
		assertEquals(LocalDateTime.of(1999, 8, 31, 23, 59, 59), addition.evaluate());
		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.DAYS, three);
		assertEquals(LocalDateTime.of(2000, 1, 3, 23, 59, 59), addition.evaluate());
		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.HOURS, negativeOne);
		assertEquals(LocalDateTime.of(1999, 12, 31, 22, 59, 59), addition.evaluate());
		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.MINUTES, two);
		assertEquals(LocalDateTime.of(2000, 1, 1, 0, 1, 59), addition.evaluate());
		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.SECONDS, three);
		assertEquals(LocalDateTime.of(2000, 1, 1, 0, 0, 2), addition.evaluate());

		addition = new DatetimeAdd(nineteenNinetyNine, ChronoUnit.YEARS, nullInteger);
		assertNull(addition.evaluate());
		addition = new DatetimeAdd(new DatetimeConstant(null), ChronoUnit.SECONDS, fifteen);
		assertNull(addition.evaluate());
	}

	public void testFromParts() {

		DatetimeFromParts fromParts;

		fromParts = new DatetimeFromParts(twentySixteen, six, fifteen, twelve, fortyFive, two, three);
		assertEquals(LocalDateTime.of(2016, 6, 15, 12, 45, 2, 3 * 1000000), fromParts.evaluate());

		fromParts = new DatetimeFromParts(nullInteger, six, fifteen, twelve, fortyFive, two, three);
		assertNull(fromParts.evaluate());
		fromParts = new DatetimeFromParts(twentySixteen, nullInteger, fifteen, twelve, fortyFive, two, three);
		assertNull(fromParts.evaluate());
		fromParts = new DatetimeFromParts(twentySixteen, six, nullInteger, twelve, fortyFive, two, three);
		assertNull(fromParts.evaluate());
		fromParts = new DatetimeFromParts(twentySixteen, six, fifteen, nullInteger, fortyFive, two, three);
		assertNull(fromParts.evaluate());
		fromParts = new DatetimeFromParts(twentySixteen, six, fifteen, twelve, nullInteger, two, three);
		assertNull(fromParts.evaluate());
		fromParts = new DatetimeFromParts(twentySixteen, six, fifteen, twelve, fortyFive, nullInteger, three);
		assertNull(fromParts.evaluate());
		fromParts = new DatetimeFromParts(twentySixteen, six, fifteen, twelve, fortyFive, two, nullInteger);
		assertNull(fromParts.evaluate());
	}

	public void testFromString() {

//		DateTimeFormatter.ofPattern("M/d/yyyy[ h:m[:s] a]"),
//		DateTimeFormatter.ofPattern("M/d/yyyy[ H:m[:s]]"),
//		DateTimeFormatter.ofPattern("yyyy-M-d[ H:m[:s]]"),
//		DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]"),


		DatetimeFromString fromString;

		String M_d_yyyy = "1/2/1993";
		fromString = new DatetimeFromString(new StringConstant(M_d_yyyy));
		assertEquals(LocalDateTime.of(1993, 1, 2, 0, 0, 0), fromString.evaluate());
		String M_d_yyyy_h_m_a = "9/17/2003 12:21:22 AM";
		fromString = new DatetimeFromString(new StringConstant(M_d_yyyy_h_m_a));
		assertEquals(LocalDateTime.of(2003, 9, 17, 0, 21, 22), fromString.evaluate());
		String M_d_yyyy_h_m_s_a = "3/4/2005 7:09:11 PM";
		fromString = new DatetimeFromString(new StringConstant(M_d_yyyy_h_m_s_a));
		assertEquals(LocalDateTime.of(2005, 3, 4, 19, 9, 11), fromString.evaluate());
		String M_d_yyyy_H_m = "10/31/2016 23:59";
		fromString = new DatetimeFromString(new StringConstant(M_d_yyyy_H_m));
		assertEquals(LocalDateTime.of(2016, 10, 31, 23, 59, 0), fromString.evaluate());
		String M_d_yyyy_H_m_s = "11/30/2017 12:01:02";
		fromString = new DatetimeFromString(new StringConstant(M_d_yyyy_H_m_s));
		assertEquals(LocalDateTime.of(2017, 11, 30, 12, 1, 2), fromString.evaluate());
		String yyyy_M_d = "1999-12-31";
		fromString = new DatetimeFromString(new StringConstant(yyyy_M_d));
		assertEquals(LocalDateTime.of(1999, 12,31, 0, 0, 0), fromString.evaluate());
		String yyyy_M_d_H_m = "2001-1-1 13:31";
		fromString = new DatetimeFromString(new StringConstant(yyyy_M_d_H_m));
		assertEquals(LocalDateTime.of(2001, 1, 1, 13, 31, 0), fromString.evaluate());
		String yyyy_M_d_H_m_s = "2020-06-07 8:00:44";
		fromString = new DatetimeFromString(new StringConstant(yyyy_M_d_H_m_s));
		assertEquals(LocalDateTime.of(2020, 6, 7, 8, 0, 44), fromString.evaluate());
		String yyyy_MM_dd_T_HH_mm_ss = "2016-04-07T04:06:57";
		fromString = new DatetimeFromString(new StringConstant(yyyy_MM_dd_T_HH_mm_ss));
		assertEquals(LocalDateTime.of(2016, 4, 7, 4, 6, 57), fromString.evaluate());
		String yyyy_MM_dd_T_HH_mm_ss_SSS = "1991-04-10T13:12:11.345";
		fromString = new DatetimeFromString(new StringConstant(yyyy_MM_dd_T_HH_mm_ss_SSS));
		assertEquals(LocalDateTime.of(1991, 4, 10, 13, 12, 11, 345 * 1000000), fromString.evaluate());

		fromString = new DatetimeFromString(new StringConstant(null));
		assertNull(fromString.evaluate());

		fromString = new DatetimeFromString(new StringConstant("not valid"));
		boolean failed = false;
		try {
			fromString.evaluate();
		}
		catch (Exception ex) {
			failed = true;
		}
		assertTrue(failed);
	}
}
