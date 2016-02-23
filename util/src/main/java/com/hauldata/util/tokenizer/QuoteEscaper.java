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

import java.util.HashMap;
import java.util.Map;

public class QuoteEscaper {

	private static Map<Character, QuoteEscaper> escapers = new HashMap<Character, QuoteEscaper>();

	private String quoteString;
	private String quoteEscaped;
	
	public static QuoteEscaper of(char quote) {

		Character escaperKey = (Character)quote;

		QuoteEscaper escaper = escapers.get(escaperKey);
		if (escaper == null) {
			escapers.put(escaperKey, (escaper = new QuoteEscaper(quote)));
		}

		return escaper;
	}

	private QuoteEscaper(char quote) {
		quoteString = String.valueOf(quote);
		quoteEscaped = quoteString + quoteString;
	}

	String escapeQuotes(String body) {
		return body.replace(quoteString, quoteEscaped);
	}
}
