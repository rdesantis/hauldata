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
import java.io.Reader;

/**
 * A subset of the Tokenizer class specialized for DSV files
 */
public class DsvTokenizer extends BaseTokenizer {

	public DsvTokenizer(Reader reader, char delimiter) {
		super(reader);

		charType[(int)'"'] = CharType.quote;
		charType[(int)delimiter] = CharType.delimiter;
		
		eolIsSignificant(true);
		splitQuoteIsAllowed(true);
		negativeIsRespected(true);
	}

	@Override
	protected Token skipWhitespace() throws IOException {
		// DSV files don't recognize whitespace.
		return Unknown.noWhitespace;
	}

	@Override
	protected Token parseWord(boolean leadingWhitespace) {
		// This should never be called.
		throw new RuntimeException("Internal error - call to DsvTokenizer.parseWord()");
	}

	@Override
	protected Delimiter parseDelimiter(boolean leadingWhitespace, char leader) {
		return new Delimiter(leadingWhitespace, String.valueOf(leader));
	}
}
