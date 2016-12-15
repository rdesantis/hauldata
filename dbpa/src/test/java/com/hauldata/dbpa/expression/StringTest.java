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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hauldata.dbpa.expression.strings.Left;
import com.hauldata.dbpa.expression.strings.LeftTrim;
import com.hauldata.dbpa.expression.strings.Lower;
import com.hauldata.dbpa.expression.strings.Replace;
import com.hauldata.dbpa.expression.strings.Replicate;
import com.hauldata.dbpa.expression.strings.Right;
import com.hauldata.dbpa.expression.strings.RightTrim;
import com.hauldata.dbpa.expression.strings.Space;
import com.hauldata.dbpa.expression.strings.Substring;
import com.hauldata.dbpa.expression.strings.Upper;

import junit.framework.TestCase;

public class StringTest extends TestCase {

	public StringTest(String name) {
		super(name);
	}

	public void testConcat() {

		final StringConstant left = new StringConstant("left");
		final StringConstant right = new StringConstant("right");
		final StringConstant nullString = new StringConstant(null);

		StringConcat concatenation;

		concatenation = new StringConcat(left, right);
		assertEquals("leftright", concatenation.evaluate());

		concatenation = new StringConcat(left, nullString);
		assertNull(concatenation.evaluate());
		concatenation = new StringConcat(nullString, right);
		assertNull(concatenation.evaluate());
		concatenation = new StringConcat(nullString, nullString);
		assertNull(concatenation.evaluate());
	}

	public void testFromDatetime() {

		final LocalDateTime christmas = LocalDateTime.of(2015, 12, 25, 1, 2, 3);
		final String pattern = "yyyy-MM-dd HH:mm:ss";

		StringFromDatetime fromDatetime;

		fromDatetime = new StringFromDatetime(new DatetimeConstant(christmas), new StringConstant(pattern));
		assertEquals(christmas.format(DateTimeFormatter.ofPattern(pattern)), fromDatetime.evaluate());

		fromDatetime = new StringFromDatetime(new DatetimeConstant(christmas), new StringConstant(null));
		assertNull(fromDatetime.evaluate());
		fromDatetime = new StringFromDatetime(new DatetimeConstant(null), new StringConstant(pattern));
		assertNull(fromDatetime.evaluate());
		fromDatetime = new StringFromDatetime(new DatetimeConstant(null), new StringConstant(null));
		assertNull(fromDatetime.evaluate());
	}

	public void testFromInteger() {

		StringFromInteger fromInteger;

		fromInteger = new StringFromInteger(new IntegerConstant(456789), new StringConstant("d"));
		assertEquals(456789, Integer.parseInt(fromInteger.evaluate()));

		fromInteger = new StringFromInteger(new IntegerConstant(456789), new StringConstant(null));
		assertNull(fromInteger.evaluate());
		fromInteger = new StringFromInteger(new IntegerConstant(null), new StringConstant("d"));
		assertNull(fromInteger.evaluate());
		fromInteger = new StringFromInteger(new IntegerConstant(null), new StringConstant(null));
		assertNull(fromInteger.evaluate());
	}

	public void testLeft() {

		final StringConstant string = new StringConstant("123456789");
		final StringConstant nullString = new StringConstant(null);

		final IntegerConstant negativeOne = new IntegerConstant(-1);
		final IntegerConstant five = new IntegerConstant(5);
		final IntegerConstant thirteen = new IntegerConstant(13);
		final IntegerConstant nullInteger = new IntegerConstant(null);

		Left left;

		left = new Left(string, five);
		assertEquals("12345", left.evaluate());
		left = new Left(string, thirteen);
		assertEquals("123456789", left.evaluate());

		left = new Left(nullString, five);
		assertNull(left.evaluate());
		left = new Left(string, nullInteger);
		assertNull(left.evaluate());

		boolean canEvaluate = true;
		try {
			left = new Left(string, negativeOne);
			left.evaluate();
		}
		catch (RuntimeException rex) {
			assertEquals("Negative length for LEFT", rex.getMessage());
			canEvaluate = false;
		}
		assertFalse(canEvaluate);
	}

	public void testLeftTrim() {

		final StringConstant leftPadded = new StringConstant("   right");
		final StringConstant rightPadded = new StringConstant("left   ");
		final StringConstant nullString = new StringConstant(null);

		LeftTrim leftTrim;

		leftTrim = new LeftTrim(leftPadded);
		assertEquals("right", leftTrim.evaluate());
		leftTrim = new LeftTrim(rightPadded);
		assertEquals("left   ", leftTrim.evaluate());
		leftTrim = new LeftTrim(nullString);
		assertNull(leftTrim.evaluate());
	}

	public void testLower() {

		final StringConstant upperString = new StringConstant("UPPER");
		final StringConstant lowerString = new StringConstant("lower");
		final StringConstant nullString = new StringConstant(null);

		Lower lower;

		lower = new Lower(upperString);
		assertEquals("upper", lower.evaluate());
		lower = new Lower(lowerString);
		assertEquals("lower", lower.evaluate());
		lower = new Lower(nullString);
		assertNull(lower.evaluate());
	}

	public void testReplace() {

		final StringConstant string = new StringConstant("This is a miss");
		final StringConstant pattern = new StringConstant("is");
		final StringConstant replacement = new StringConstant("at");
		final StringConstant emptyString = new StringConstant("");
		final StringConstant nullString = new StringConstant(null);

		Replace replace;

		replace = new Replace(string, pattern, replacement);
		assertEquals("That at a mats", replace.evaluate());
		replace = new Replace(string, replacement, pattern);
		assertEquals("This is a miss", replace.evaluate());
		replace = new Replace(nullString, pattern, replacement);
		assertNull(replace.evaluate());
		replace = new Replace(string, nullString, replacement);
		assertNull(replace.evaluate());
		replace = new Replace(string, pattern, nullString);
		assertNull(replace.evaluate());

		boolean canEvaluate = true;
		try {
			replace = new Replace(string, emptyString, replacement);
			replace.evaluate();
		}
		catch (RuntimeException rex) {
			assertEquals("Empty string used for REPLACE pattern", rex.getMessage());
			canEvaluate = false;
		}
		assertFalse(canEvaluate);
	}

	public void testReplicate() {

		final StringConstant string = new StringConstant("abc");
		final StringConstant nullString = new StringConstant(null);

		final IntegerConstant negativeOne = new IntegerConstant(-1);
		final IntegerConstant zero = new IntegerConstant(0);
		final IntegerConstant one = new IntegerConstant(1);
		final IntegerConstant five = new IntegerConstant(5);
		final IntegerConstant nullInteger = new IntegerConstant(null);

		Replicate replicate;

		replicate = new Replicate(string, negativeOne);
		assertNull(replicate.evaluate());
		replicate = new Replicate(string, zero);
		assertEquals("", replicate.evaluate());
		replicate = new Replicate(string, one);
		assertEquals("abc", replicate.evaluate());
		replicate = new Replicate(string, five);
		assertEquals("abcabcabcabcabc", replicate.evaluate());
		replicate = new Replicate(nullString, five);
		assertNull(replicate.evaluate());
		replicate = new Replicate(string, nullInteger);
		assertNull(replicate.evaluate());
	}

	public void testRight() {

		final StringConstant string = new StringConstant("123456789");
		final StringConstant nullString = new StringConstant(null);

		final IntegerConstant negativeOne = new IntegerConstant(-1);
		final IntegerConstant five = new IntegerConstant(5);
		final IntegerConstant thirteen = new IntegerConstant(13);
		final IntegerConstant nullInteger = new IntegerConstant(null);

		Right right;

		right = new Right(string, five);
		assertEquals("56789", right.evaluate());
		right = new Right(string, thirteen);
		assertEquals("123456789", right.evaluate());

		right = new Right(nullString, five);
		assertNull(right.evaluate());
		right = new Right(string, nullInteger);
		assertNull(right.evaluate());

		boolean canEvaluate = true;
		try {
			right = new Right(string, negativeOne);
			right.evaluate();
		}
		catch (RuntimeException rex) {
			assertEquals("Negative length for RIGHT", rex.getMessage());
			canEvaluate = false;
		}
		assertFalse(canEvaluate);
	}

	public void testRightTrim() {

		final StringConstant leftPadded = new StringConstant("   right");
		final StringConstant rightPadded = new StringConstant("left   ");
		final StringConstant nullString = new StringConstant(null);

		RightTrim rightTrim;

		rightTrim = new RightTrim(leftPadded);
		assertEquals("   right", rightTrim.evaluate());
		rightTrim = new RightTrim(rightPadded);
		assertEquals("left", rightTrim.evaluate());
		rightTrim = new RightTrim(nullString);
		assertNull(rightTrim.evaluate());
	}

	public void testSpace() {

		final IntegerConstant negativeOne = new IntegerConstant(-1);
		final IntegerConstant zero = new IntegerConstant(0);
		final IntegerConstant one = new IntegerConstant(1);
		final IntegerConstant five = new IntegerConstant(5);
		final IntegerConstant nullInteger = new IntegerConstant(null);

		Space space;

		space = new Space(negativeOne);
		assertNull(space.evaluate());
		space = new Space(zero);
		assertEquals("", space.evaluate());
		space = new Space(one);
		assertEquals(" ", space.evaluate());
		space = new Space(five);
		assertEquals("     ", space.evaluate());
		space = new Space(nullInteger);
		assertNull(space.evaluate());
	}

	public void testSubstring() {

		final StringConstant string = new StringConstant("123456789");
		final StringConstant nullString = new StringConstant(null);

		final IntegerConstant negativeFive = new IntegerConstant(-5);
		final IntegerConstant negativeOne = new IntegerConstant(-1);
		final IntegerConstant zero = new IntegerConstant(0);
		final IntegerConstant one = new IntegerConstant(1);
		final IntegerConstant five = new IntegerConstant(5);
		final IntegerConstant nine = new IntegerConstant(9);
		final IntegerConstant thirteen = new IntegerConstant(13);
		final IntegerConstant nullInteger = new IntegerConstant(null);

		Substring substring;

		substring = new Substring(string, one, five);
		assertEquals("12345", substring.evaluate());
		substring = new Substring(string, one, nine);
		assertEquals("123456789", substring.evaluate());
		substring = new Substring(string, one, thirteen);
		assertEquals("123456789", substring.evaluate());
		substring = new Substring(string, five, one);
		assertEquals("5", substring.evaluate());
		substring = new Substring(string, five, thirteen);
		assertEquals("56789", substring.evaluate());
		substring = new Substring(string, five, zero);
		assertEquals("", substring.evaluate());
		substring = new Substring(string, thirteen, five);
		assertEquals("", substring.evaluate());
		substring = new Substring(string, negativeOne, five);
		assertEquals("123", substring.evaluate());
		substring = new Substring(string, negativeOne, thirteen);
		assertEquals("123456789", substring.evaluate());
		substring = new Substring(string, negativeFive, one);
		assertEquals("", substring.evaluate());

		substring = new Substring(nullString, one, five);
		assertNull(substring.evaluate());
		substring = new Substring(string, nullInteger, five);
		assertNull(substring.evaluate());
		substring = new Substring(string, one, nullInteger);
		assertNull(substring.evaluate());

		boolean canEvaluate = true;
		try {
			substring = new Substring(string, five, negativeOne);
			substring.evaluate();
		}
		catch (RuntimeException rex) {
			assertEquals("Negative length for SUBSTRING", rex.getMessage());
			canEvaluate = false;
		}
		assertFalse(canEvaluate);
	}

	public void testUpper() {

		final StringConstant upperString = new StringConstant("UPPER");
		final StringConstant lowerString = new StringConstant("lower");
		final StringConstant nullString = new StringConstant(null);

		Upper upper;

		upper = new Upper(upperString);
		assertEquals("UPPER", upper.evaluate());
		upper = new Upper(lowerString);
		assertEquals("LOWER", upper.evaluate());
		upper = new Upper(nullString);
		assertNull(upper.evaluate());
	}
}
