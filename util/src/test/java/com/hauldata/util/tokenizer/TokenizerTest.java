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
import java.math.BigInteger;
import java.util.NoSuchElementException;

import junit.framework.TestCase;

public class TokenizerTest extends TestCase {

	public TokenizerTest(String name) {
		super(name);
	}

	private static void assertHasNext(Tokenizer tokenizer, Class<?> tokenClass) throws IOException {
		assertTrue(tokenizer.hasNext());
		assertEquals(tokenClass.equals(Word.class), tokenizer.hasNextWord());
		assertEquals(tokenClass.equals(Integer.class), tokenizer.hasNextInt());
		assertEquals(tokenClass.equals(Quoted.class), tokenizer.hasNextQuoted());
		assertEquals(tokenClass.equals(Delimiter.class), tokenizer.hasNextDelimiter());
	}

	private static void assertNextWord(Tokenizer tokenizer, String word) throws IOException {
		assertHasNext(tokenizer, Word.class);
		assertTrue(tokenizer.hasNextWordIgnoreCase(word.toLowerCase()));
		assertTrue(tokenizer.hasNextWordIgnoreCase(word.toUpperCase()));
		assertEquals(word, tokenizer.nextWord());
	}

	private static void assertNextInt(Tokenizer tokenizer, int value) throws IOException {
		assertHasNext(tokenizer, Integer.class);
		assertEquals(value, tokenizer.nextInt());
	}

	private static void assertNextLong(Tokenizer tokenizer, long value) throws IOException {
		assertHasNext(tokenizer, Long.class);
		Token token = tokenizer.nextToken();
		assertEquals(Numeric.class, token.getClass());
		Object tokenValue = token.getValue();
		assertEquals(Long.class, tokenValue.getClass());
		assertEquals(value, ((Long)tokenValue).longValue());
	}

	private static void assertNextQuoted(Tokenizer tokenizer, char quote, String body) throws IOException {
		assertHasNext(tokenizer, Quoted.class);
		Quoted quoted = new Quoted(true, quote, body);
		assertEquals(quoted, tokenizer.nextQuoted());
	}

	private static void assertNextDelimiter(Tokenizer tokenizer, String value) throws IOException {
		assertHasNext(tokenizer, Delimiter.class);
		assertTrue(tokenizer.hasNextDelimiter(value));
		Delimiter delimiter = new Delimiter(true, value);
		assertEquals(delimiter, tokenizer.nextDelimiter());
	}

	private static void assertNextNumeric(Tokenizer tokenizer, Class<? extends Number> numberClass, Number value) throws IOException {
		assertHasNext(tokenizer, numberClass);
		Token token = tokenizer.nextToken();
		Object tokenValue = token.getValue();
		assertEquals(numberClass, tokenValue.getClass());
		if (numberClass.equals(BigInteger.class)) {
			assertEquals(0, ((BigInteger)value).compareTo((BigInteger)tokenValue));
		}
		else if (numberClass.equals(BigDecimal.class)) {
			assertEquals(0, ((BigDecimal)value).compareTo((BigDecimal)tokenValue));
		}
		else {
			assertEquals(value, token.getValue());
		}
	}

	static final String text =
			"This is 2 'tests' (combined; sort of, into 1.1)! 'NOT REALLY!' -- not really \r\n" +
			"Line 222222222222 9223372036854775808 2E22";

	private static void testTokenizerLine1(Tokenizer tokenizer) throws IOException {

		assertNextWord(tokenizer, "This");
		assertNextWord(tokenizer, "is");
		assertNextInt(tokenizer, 2);
		assertNextQuoted(tokenizer, '\'', "tests");
		assertNextDelimiter(tokenizer, "(");
		assertNextWord(tokenizer, "combined");
		assertNextDelimiter(tokenizer, ";");
		assertNextWord(tokenizer, "sort");
		assertNextWord(tokenizer, "of");
		assertNextDelimiter(tokenizer, ",");
		assertNextWord(tokenizer, "into");
		assertNextNumeric(tokenizer, BigDecimal.class, new BigDecimal("1.1"));
		assertNextDelimiter(tokenizer, ")");
		assertNextDelimiter(tokenizer, "!");
		assertNextQuoted(tokenizer, '\'', "NOT REALLY!");
	}
	
	private static void testTokenizerLine2(Tokenizer tokenizer) throws IOException {

		assertNextWord(tokenizer, "Line");
		assertNextLong(tokenizer, 222222222222L);
		assertNextNumeric(tokenizer, BigInteger.class, new BigInteger("9223372036854775808"));
		assertNextNumeric(tokenizer, BigDecimal.class, new BigDecimal("2E22"));
	}

	private static void testTokenizerWithComment(Tokenizer tokenizer, String delimiter) throws IOException {

		tokenizer.eolIsSignificant(true);
		tokenizer.useEndLineCommentDelimiter(delimiter);

		testTokenizerLine1(tokenizer);

		assertFalse(tokenizer.hasNextOnLine());
		assertHasNext(tokenizer, EndOfLine.class);
		assertEquals(EndOfLine.value, tokenizer.nextToken());

		testTokenizerLine2(tokenizer);

		assertFalse(tokenizer.hasNextOnLine());
		assertFalse(tokenizer.hasNext());
		assertNull(tokenizer.nextToken());
	}

	private static void testTokenizerWithoutComment(Tokenizer tokenizer) throws IOException {

		tokenizer.eolIsSignificant(false);

		testTokenizerLine1(tokenizer);

		assertNextDelimiter(tokenizer, "-");
		assertNextDelimiter(tokenizer, "-");
		assertNextWord(tokenizer, "not");
		assertNextWord(tokenizer, "really");

		testTokenizerLine2(tokenizer);

		assertFalse(tokenizer.hasNext());
		assertNull(tokenizer.nextToken());
	}

	public void testTokenizer() throws IOException {

		testTokenizerWithComment(new Tokenizer(new StringReader(text)), "--");
		testTokenizerWithoutComment(new Tokenizer(new StringReader(text)));
		
		final String skipText = "lower + UPPER";
		Tokenizer tokenizer = new Tokenizer(new StringReader(skipText));
		
		assertFalse(tokenizer.skipWordIgnoreCase("Garbage"));
		assertTrue(tokenizer.skipWordIgnoreCase("LOWER"));
		assertFalse(tokenizer.skipWordIgnoreCase("Garbage"));
		assertFalse(tokenizer.skipDelimiter("-"));
		assertTrue(tokenizer.skipDelimiter("+"));
		assertFalse(tokenizer.skipWordIgnoreCase("Garbage"));
		assertTrue(tokenizer.skipWordIgnoreCase("upper"));
		assertNull(tokenizer.nextToken());
	}

	public void testBacktrackingTokenizer() throws IOException {

		testTokenizerWithComment(new BacktrackingTokenizer(new StringReader(text)), "--");
		testTokenizerWithoutComment(new BacktrackingTokenizer(new StringReader(text)));
		
		final String backtrackedText = "This is 2 a test;";
		BacktrackingTokenizer tokenizer = new BacktrackingTokenizer(new StringReader(backtrackedText));

		assertNextWord(tokenizer, "This");
		BacktrackingTokenizerMark mark = tokenizer.mark();

		assertNextWord(tokenizer, "is");
		assertNextInt(tokenizer, 2);
		tokenizer.reset(mark);

		assertNextWord(tokenizer, "is");
		assertNextInt(tokenizer, 2);
		assertNextWord(tokenizer, "a");
		assertNextWord(tokenizer, "test");
		assertNextDelimiter(tokenizer, ";");

		assertFalse(tokenizer.hasNext());
		assertNull(tokenizer.nextToken());
	}

	public void testRender() throws IOException {

		final String text =
				"INTO\n" +
				"	EXEC StoredProc\n" +
				"		@first_arg = ?,\n" +
				"		@second_arg= ? ,\n" +
				"		@third_arg=name@domain.com\n" +
				"	Garbage  @   function(arg) nonfunction (   nonarg   )   a@b+-  -   -- Ignore\n" +
				"END TASK";

		BacktrackingTokenizer tokenizer = new BacktrackingTokenizer(new StringReader(text));
		tokenizer.useEndLineCommentDelimiter("--");

		assertNextWord(tokenizer, "INTO");

		BacktrackingTokenizerMark mark = tokenizer.mark();

		StringBuilder renderedStatement = new StringBuilder();
		while (!hasEndTask(tokenizer)) {
			Token nextToken = tokenizer.nextToken();
			renderedStatement.append(nextToken.render());

			if (!tokenizer.hasNext()) {
				tokenizer.reset(mark);
				throw new NoSuchElementException("SQL not terminated properly at line " + String.valueOf(tokenizer.lineno()));
			}
		}

		final String normalizedStatement =
				" EXEC StoredProc" +
				" @first_arg = ?," +
				" @second_arg= ? ," +
				" @third_arg=name@domain.com" +
				" Garbage @ function(arg) nonfunction ( nonarg ) a@b+- -";

		assertEquals(normalizedStatement, renderedStatement.toString());
	}

	private boolean hasEndTask(BacktrackingTokenizer tokenizer) throws IOException {

		if (!tokenizer.hasNextWordIgnoreCase("END")) {
			return false;
		}

		BacktrackingTokenizerMark mark = tokenizer.mark();
		tokenizer.nextToken();

		boolean result = tokenizer.hasNextWordIgnoreCase("TASK");

		tokenizer.reset(mark);
		return result;
	}
}
