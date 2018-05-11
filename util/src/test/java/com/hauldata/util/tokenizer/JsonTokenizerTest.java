/*
 * Copyright (c) 2018, Ronald DeSantis
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

import junit.framework.TestCase;

public class JsonTokenizerTest extends TestCase {

	public JsonTokenizerTest(String name) {
		super(name);
	}

	public void testTokenizer() throws IOException {

		final String text = "[ { \"quoted\" : 12345, name: \"Escapes: '\\\"\\\\\\/\\b\\f\\n\\r\\t\\u7aF0'\"},{}]";
		Tokenizer tokenizer = new JsonTokenizer(new StringReader(text));

		tokenizer.nextDelimiter("[");
		tokenizer.nextDelimiter("{");

		Quoted quoted = tokenizer.nextQuoted();
		assertEquals('"', quoted.getQuote());
		assertEquals("quoted", quoted.getBody());

		tokenizer.nextDelimiter(":");
		assertEquals(12345, tokenizer.nextInt());
		tokenizer.nextDelimiter(",");
		assertEquals("name", tokenizer.nextWord());
		tokenizer.nextDelimiter(":");

		quoted = tokenizer.nextQuoted();
		assertEquals('"', quoted.getQuote());
		assertEquals("Escapes: '\"\\/\b\f\n\r\t\u7aF0'", quoted.getBody());

		tokenizer.nextDelimiter("}");
		tokenizer.nextDelimiter(",");
		tokenizer.nextDelimiter("{");
		tokenizer.nextDelimiter("}");
		tokenizer.nextDelimiter("]");

		assertFalse(tokenizer.hasNext());
	}
}
