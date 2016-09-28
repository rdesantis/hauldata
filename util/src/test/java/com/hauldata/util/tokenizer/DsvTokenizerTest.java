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

package com.hauldata.util.tokenizer;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.InputMismatchException;

import junit.framework.TestCase;

public class DsvTokenizerTest extends TestCase {

	public DsvTokenizerTest(String name) {
		super(name);
	}

	public void testDsvTokenizer() throws InputMismatchException, IOException {

		final String text =
				"One,\"Two two\",,4,-555555555555555,6.6E6\r\n" +
				"END\r\n";

		DsvTokenizer tokenizer = new DsvTokenizer(new StringReader(text), ',');
		Delimiter comma = new Delimiter(false, ",");

		assertEquals(1, tokenizer.lineno());
		assertEquals(new Unknown(false, "One"), tokenizer.nextToken());
		assertEquals(comma, tokenizer.nextToken());
		assertEquals(new Quoted(false, '"', "Two two"), tokenizer.nextToken());
		assertEquals(comma, tokenizer.nextToken());
		assertEquals(comma, tokenizer.nextToken());
		assertEquals(new Numeric<Integer>(false, "4", 4), tokenizer.nextToken());
		assertEquals(comma, tokenizer.nextToken());
		assertEquals(new Numeric<Long>(false, "-555555555555555", new Long(-555555555555555L)), tokenizer.nextToken());
		assertEquals(comma, tokenizer.nextToken());
		assertEquals(new Numeric<BigDecimal>(false, "6.6E6", new BigDecimal("6.6E6")), tokenizer.nextToken());
		
		assertFalse(tokenizer.hasNextOnLine());
		assertEquals(EndOfLine.value, tokenizer.nextToken());

		assertEquals(2, tokenizer.lineno());
		assertEquals(new Unknown(false, "END"), tokenizer.nextToken());

		assertFalse(tokenizer.hasNextOnLine());
		assertEquals(EndOfLine.value, tokenizer.nextToken());

		assertFalse(tokenizer.hasNext());
		assertNull(tokenizer.nextToken());
	}
}
