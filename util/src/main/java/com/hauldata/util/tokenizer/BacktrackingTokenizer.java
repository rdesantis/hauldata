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
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.ListIterator;

/**
 * The <code>BacktrackingTokenizer</code> class extends the
 * <code>Tokenizer</code> class with the ability to mark
 * a position in the input stream, scan any number of
 * tokens past the mark, and then reset the input stream
 * to resume scanning at the mark.  An unlimited number of
 * position marks can be defined concurrently.  This allows for
 * scanning in situations where it may be necessary to look ahead
 * an indeterminate number of tokens into the input stream.
 * <p>
 * In this implementation, resetting to a mark position
 * does not actually reset the underlying stream.  Instead, as
 * tokens are scanned they are saved in a list.  Resetting to
 * a mark position actually resets the position in the saved token
 * list.  Thus, reset is a very efficient operation.  However,
 * scanning of a very large input stream can result in the creation
 * of a large list of tokens.
 */
public class BacktrackingTokenizer extends Tokenizer {

	private List<Token> tokens;
	private ListIterator<Token> position;

	public BacktrackingTokenizer(Reader reader) {
		super(reader);

		tokens = new ArrayList<Token>();
		position = tokens.listIterator();
	}

	/**
	 * Returns a mark indicating the position of the input stream.
	 * The input stream can subsequently be scanned (e.g., with
	 * <code>nextToken()</code>) and then reset to the mark position
	 * using the <code>reset()</code> member.
	 * 
	 * @return	a mark indicating the position of the input stream
	 * @see		BacktrackingTokenizer#reset(ResetableTokenScannerMark)
	 */
	public BacktrackingTokenizerMark mark() {
		return new BacktrackingTokenizerMark(tokens, position);
	}

	/**
	 * Resets the input stream to the position it had at the time of
	 * an earlier call to <code>mark()</code>
	 * 
	 * @param	mark	is a mark returned by a previous call to
	 *			mark() for this same token scanner.
	 * @see		BacktrackingTokenizer#mark()
	 */
	public void reset(BacktrackingTokenizerMark mark) {
		position = mark.getPosition(tokens);
	}

	// Base class overrides - implementation details

	@Override
	public Token nextToken() throws IOException, InputMismatchException {

		if (position.hasNext()) {
			return position.next();
		}
		else {
			Token token = super.getToken();
			if (token != null) {
				position.add(token);
			}
			return token;
		}
	}

	@Override
	protected Token peekToken() throws IOException, InputMismatchException {

		Token token = nextToken();
		if (token != null) {
			position.previous();
		}
		return token;
	}
}
