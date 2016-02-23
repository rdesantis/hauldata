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

import java.util.List;
import java.util.ListIterator;

public class BacktrackingTokenizerMark {
	private int index;

	// Constructor and method have package visibility.
	// Only BacktrackingTokenizer needs to use them.

	BacktrackingTokenizerMark(List<Token> tokens, ListIterator<Token> position) {
		// Cannot simply retain a reference to the caller's iterator, because we must
		// retain position indicator that will not change its position when the caller
		// subsequently changes the position of the argument iterator.
		index = position.nextIndex();
	}

	ListIterator<Token> getPosition(List<Token> tokens) {
		return tokens.listIterator(index);
	}
}
