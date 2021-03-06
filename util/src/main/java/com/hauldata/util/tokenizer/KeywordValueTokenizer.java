/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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

import java.io.Reader;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A tokenizer that only recognizes whitespace-separated alphanumeric keywords and double-quoted strings.
 * Everything else is parsed as a token of unknown type.  In particular, delimiter characters are not
 * specially recognized and are simply combined with all adjacent characters into a single token.
 */
public class KeywordValueTokenizer extends Tokenizer {

	public KeywordValueTokenizer(Reader reader) {
		super(reader);

		setCharType(CharType.unknown, '\'');
		setCharType(CharType.quote, '"');
		setCharType(CharType.alphabetic, '_');

		CharType[] recognizedArray = new CharType[] { CharType.alphabetic, CharType.whitespace, CharType.quote };
		Set<CharType> recognizedSet = Arrays.stream(recognizedArray).collect(Collectors.toSet());

		for (int i = 0; i <= maxSupportedCharCode; ++i) {
			if (!recognizedSet.contains(getCharType((char)i))) {
				setCharType(CharType.unknown, (char)i);
			}
		}
	}
}
