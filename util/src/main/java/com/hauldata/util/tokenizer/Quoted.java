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

public class Quoted extends Token {
	private char quote;
	private String body;
	
	private QuoteEscaper escaper;

	public Quoted(boolean leadingWhitespace, char quote, String body) {
		super(leadingWhitespace);
		this.quote = quote;
		this.body = body;
		
		escaper = QuoteEscaper.of(quote);
	}

	@Override
	public String getImage() {
		return (String)getValue();
	}

	public char getQuote() {
		return quote;
	}

	public String getBody() {
		return body;
	}

	@Override
	public Object getValue() {
		return quote + escaper.escapeQuotes(body) + quote;
	}

	@Override
	public int hashCode() {
		return body.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return (obj != null) && (obj instanceof Quoted) && (((Quoted)obj).quote == quote) && (((Quoted)obj).body.equals(body));
	}
}
