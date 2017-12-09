/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hauldata.dbpa.file.PageOptions.Modifier;
import com.hauldata.dbpa.process.TaskSetParser;
import com.hauldata.util.tokenizer.BacktrackingTokenizer;
import com.hauldata.util.tokenizer.BacktrackingTokenizerMark;

/**
 * Parser target or source options for a page type
 *
 * A subclass provides a map of option keywords to option modifiers and this
 * abstract base class implements the parsing.  This class provides for
 * options to be specified at multiple levels in a class hierarchy.
 */
abstract class PageOptionsParser implements PageOptions.Parser {

	private Map<String[], PageOptions.Modifier> modifiers;

	protected PageOptionsParser(Map<String, PageOptions.Modifier> modifiers) {

		this.modifiers = new HashMap<String[], PageOptions.Modifier>();
		for (Map.Entry<String, PageOptions.Modifier> modifier : modifiers.entrySet()) {
			String[] tokens = modifier.getKey().split(" ");
			this.modifiers.put(tokens, modifier.getValue());
		}
	}

	protected static Map<String, PageOptions.Modifier> combine(
			Map<String, PageOptions.Modifier> superModifiers,
			Map<String, PageOptions.Modifier> subModifiers) {

		Map<String, PageOptions.Modifier> modifiers = new HashMap<String, Modifier>(superModifiers);
		modifiers.putAll(subModifiers);
		return modifiers;
	}

	protected abstract PageOptions makeDefaultOptions();

	@Override
	public PageOptions parse(TaskSetParser parser) throws IOException {

		BacktrackingTokenizer tokenizer = parser.getTokenizer();
		PageOptions options = makeDefaultOptions();

		boolean matchedAnyOption;
		do {
			matchedAnyOption = false;
			BacktrackingTokenizerMark mark = tokenizer.mark();

			for (Map.Entry<String[], PageOptions.Modifier> modifier : modifiers.entrySet()) {

				boolean matchedAllWords = true;
				for (String word : modifier.getKey()) {
					if (!tokenizer.skipWordIgnoreCase(word)) {
						matchedAllWords = false;
						break;
					}
				}
				if (matchedAllWords) {
					matchedAnyOption = true;
					modifier.getValue().modify(parser, options);
					break;
				}
				else {
					tokenizer.reset(mark);
				}
			}
		} while (matchedAnyOption);

		return options;
	}
}
