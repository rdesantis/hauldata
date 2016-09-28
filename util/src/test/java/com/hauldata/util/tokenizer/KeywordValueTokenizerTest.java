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
import java.util.InputMismatchException;

import junit.framework.TestCase;

public class KeywordValueTokenizerTest extends TestCase {

	public KeywordValueTokenizerTest(String name) {
		super(name);
	}

	public void testPositive() throws InputMismatchException, IOException {

		final String text =
				"driver com.mysql.jdbc.Driver url jdbc:mysql://localhost/test allowMultiQueries true whatever \"stuff in quotes\"";

		KeywordValueTokenizer tokenizer = new KeywordValueTokenizer(new StringReader(text));

		assertEquals(1, tokenizer.lineno());
		assertEquals("driver", tokenizer.nextWord());
		assertEquals(new Unknown(true, "com.mysql.jdbc.Driver"), tokenizer.nextToken());
		assertEquals("url", tokenizer.nextWord());
		assertEquals(new Unknown(true, "jdbc:mysql://localhost/test"), tokenizer.nextToken());
		assertEquals("allowMultiQueries", tokenizer.nextWord());
		assertEquals("true", tokenizer.nextWord());
		assertEquals("whatever", tokenizer.nextWord());
		assertEquals(new Quoted(true, '"', "stuff in quotes"), tokenizer.nextToken());

		assertFalse(tokenizer.hasNext());
		assertNull(tokenizer.nextToken());
	}

	public void testBadKeyword() throws IOException {

		final String text =
				"one two buckle-my shoe";

		KeywordValueTokenizer tokenizer = new KeywordValueTokenizer(new StringReader(text));

		tokenizer.nextWord();
		tokenizer.nextToken();

		boolean caughtError = false;
		try {
			tokenizer.nextWord();
		}
		catch (InputMismatchException ex) {
			caughtError = true;
		}

		assertTrue(caughtError);
	}

	public void testMissingValue() throws IOException {

		final String text =
				"one two buckle_my_shoe";

		KeywordValueTokenizer tokenizer = new KeywordValueTokenizer(new StringReader(text));

		tokenizer.nextWord();
		tokenizer.nextToken();
		tokenizer.nextWord();

		assertNull(tokenizer.nextToken());
	}
}
