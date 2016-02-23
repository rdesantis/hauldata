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
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

public class Tokenizer extends BaseTokenizer {

	private List<String> compoundDelimiters;
	private String endLineCommentDelimiter;

	public Tokenizer(Reader reader) {
		super(reader);

		charType[(int)'\t'] = CharType.whitespace;
		charType[(int)' '] = CharType.whitespace;

		int i;
		for (i = 33; i <= 47; ++i) {
			charType[i] = CharType.delimiter;
		}
		for (i = 58; i <= 64; ++i) {
			charType[i] = CharType.delimiter;
		}
		for (i = (int)'A'; i <= (int)'Z'; ++i) {
			charType[i] = CharType.alphabetic;
		}
		for (i = 91; i <= 96; ++i) {
			charType[i] = CharType.delimiter;
		}
		for (i = (int)'a'; i <= (int)'z'; ++i) {
			charType[i] = CharType.alphabetic;
		}
		for (i = 123; i <= 126; ++i) {
			charType[i] = CharType.delimiter;
		}

		charType[(int)'\''] = CharType.quote;

		eolIsSignificant(false);
		splitQuoteIsAllowed(false);
		negativeIsRespected(false);
		
		compoundDelimiters = new LinkedList<String>();
		endLineCommentDelimiter = null;
	}

	/**
     * @return	this tokenizer
     * @see		java.io.StreamTokenizer#wordChars(int, int)
	 */
	public Tokenizer wordChars(int low, int hi) {
		for (int i = low; i <= hi; ++i) {
			charType[i] = CharType.alphabetic;
		}
		return this;
	}

    /**
 	 * Adds a multi-character delimiter to the set of character
     * sequences recognized as delimiters.
 	 * <p>
	 * By default any character that is not whitespace, word, number, or end-of-line
	 * character is considered to be a delimiter character.  (The quote character
	 * is a special delimiter as well.)  This member function allows defining
	 * multi-character delimiters.  Each character of a multi-character
	 * delimiter must itself be a delimiter character.  In addition, if the multi-
	 * character delimiter consists of three or more characters, than each leading
	 * sequence of two or more characters will itself be considered a delimiter.
	 * The longest possible matching sequence of scanned characters against any
	 * of the specified multi-character delimiters will tested until one of the
	 * specified mulit-character delimiters is matched completely.
	 *
     * @param	delimiter is the character sequence for the multi-character delimiter
     * @return	this tokenizer
     */
	public Tokenizer useDelimiter(String delimiter) {
		compoundDelimiters.add(delimiter);
		return this;
	}

	/**
	 * Allow end line comments introduced by a delimiter, which may be a multi-character
	 * delimiter.
	 * @param delimiter
	 * @return
	 */
	public Tokenizer useEndLineCommentDelimiter(String delimiter) {
		if (delimiter.length() > 1) {
			compoundDelimiters.add(delimiter);
		}
		endLineCommentDelimiter = delimiter;
		return this;
	}

    /**
     * Returns true if the next token in this tokenizer's input is a valid
     * identifier consisting of word characters.  The tokenizer does not
     * advance past any input.
     *
     * @return	true if and only if this tokenizer's next token is a valid
     *			identifier
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean hasNextWord() throws IOException {
		return (peekToken() instanceof Word);
	}

    /**
     * Returns true if the next token in this tokenizer's input is a valid
     * identifier that matches the argument word ignoring case.
     * The tokenizer does not advance past any input.
     * 
     * @return	true if and only if this tokenizer's next token is a valid
     *			identifier matching the argument word
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean hasNextWordIgnoreCase(String word) throws IOException {
		return hasNextWord() && peekToken().toString().equalsIgnoreCase(word);
	}

    /**
     * Returns true if the next token in this tokenizer's input is a
     * quoted text.  The tokenizer does not advance past any input.
     *
     * @return	true if and only if this tokenizer's next token is
     *			quoted text
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean hasNextQuoted() throws IOException {
		return (peekToken() instanceof Quoted);
	}

    /**
     * Returns true if the next token in this tokenizer's input is a
     * specific non-whitespace delimiter.  The tokenizer does not advance past any input.
     *
     * @param	delimiter is the delimiter that the next token in
     *			the input stream must match.  If this is a multi-character delimiter,
     *			<code>Tokenizer.useDelimiter</code> must have been
     *			called previously with this delimiter.
     * @return	true if and only if this tokenizer's next token is the
     *			indicated non-whitespace delimiter
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean hasNextDelimiter(String delimiter) throws IOException, InputMismatchException {
		return hasNextDelimiter() && peekToken().toString().equals(delimiter);
	}

    /**
     * Returns true if the next token in this tokenizer's input can be
     * interpreted as an int value.  The scanner does not advance past any input.
     *
     * @return	true if and only if this scanner's next token is a valid
     *			int value
     * @throws	IOException if passed by the underlying
     *			<code>java.io.StreamTokenizer</code> object of this scanner.
     */
	public boolean hasNextInt() throws IOException {
		return (peekToken().getValue() instanceof Integer);
	}

    /**
     * Scans the next token of the input as an identifier
     * consisting of word characters.
     *
     * @return	a String containing the identifier scanned from the input
     * @throws	InputMismatchException if the next token
     *			is not an identifier
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public String nextWord() throws IOException, InputMismatchException, NoSuchElementException {
		Token token = nextToken();
		if (!(token instanceof Word)) {
			throwInputMismatchOrNoSuchElementException(token, "identifier");
		}
		return token.toString();
	}

	public String nextWordUpperCase() throws IOException, InputMismatchException, NoSuchElementException {
		return nextWord().toUpperCase();
	}

	/**
     * Scans the next token of the input as quoted text.
     *
     * @return	a <code>Quoted</code> object containing the quoted text
     *			scanned from the input.  The <code>quote</code> data member
     *			gives the quotation mark that enclosed the text, and the
     *			<code>body</code> data member gives the enclosed text.
     * @throws	InputMismatchException if the next token
     *			is not quoted text
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public Quoted nextQuoted() throws IOException, InputMismatchException, NoSuchElementException {
		Object token = nextToken();
		if (!(token instanceof Quoted)) {
			throwInputMismatchOrNoSuchElementException(token, "quoted text");
		}
		return (Quoted)token;
	}

    /**
     * Scans the next token of the input as a delimiter.
     *
     * @return	a <code>Delimiter</code> object containing the delimiter
     *			scanned from the input.  The <code>value</code> data member
     *			is a string containing the delimiter, which may have multiple
     *			characters.
     * @throws	InputMismatchException if the next token
     *			is not a delimiter
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     * @see		Tokenizer#useDelimiter(String)
     */
	public Delimiter nextDelimiter() throws IOException, InputMismatchException, NoSuchElementException {
		Object token = nextToken();
		if (!(token instanceof Delimiter)) {
			throwInputMismatchOrNoSuchElementException(token, "delimiter");
		}
		return (Delimiter)token;
	}

    /**
     * Scans and discards the next token of the input,
     * which must match a specific delimiter.
     *
     * @param	delimiter	is the delimiter that the next token in
     *			the input stream must match.  If this is a
     *			multi-character delimiter,
     *			<code>TokenScanner.useDelimiter</code> must have been
     *			called previously with a pattern which this delimiter
     *			will match.
     * @throws	InputMismatchException if the next token is not
     *			the specified delimiter
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     * @see		Tokenizer#useDelimiter(String)
     */
	public void nextDelimiter(String delimiter) throws IOException, InputMismatchException, NoSuchElementException {
		Delimiter found = nextDelimiter();
		if (!found.toString().equals(delimiter)) {
			throwInputMismatchOrNoSuchElementException(found, "\"" + delimiter + "\"");
		}
	}

	/**
     * Scans the next token of the input as an <tt>int</tt>.
     *
     * @return	the <tt>int</tt> scanned from the input
     * @throws	InputMismatchException if the next token
     *			cannot be interpreted as an <tt>int</tt>
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public int nextInt() throws IOException, InputMismatchException, NoSuchElementException {
		Token token = nextToken();
		if (!(token.getValue() instanceof Integer)) {
			throwInputMismatchOrNoSuchElementException(token, "integer");
		}
		return (Integer)token.getValue();
	}

    /**
     * Scans and discards the next token of the input if and only if
     * it is a valid identifier that matches the argument word ignoring case.
     *
     * @return	true if and only if this tokenizer's next token
     *			matches the argument word ignoring case
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean skipWordIgnoreCase(String word) throws IOException {
		boolean skip = hasNextWord() && peekToken().toString().equalsIgnoreCase(word);
		if (skip) nextToken();
		return skip;
	}

    /**
     * Scans and discards the next token of the input if and only if
     * it matches the argument delimiter.
     *
     * @return	true if and only if this tokenizer's next token
     *			matches the argument delimiter
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean skipDelimiter(String delimiter) throws IOException {
		boolean skip = hasNextDelimiter() && peekToken().toString().equals(delimiter);
		if (skip) nextToken();
		return skip;
	}

	private void throwInputMismatchOrNoSuchElementException(Object token, String expecting)
			throws InputMismatchException, NoSuchElementException {
		String message = "At line " + lineno() + " expecting " + expecting; 
		if (token == null)
			throw new NoSuchElementException(message);
		else
			throw new InputMismatchException(message);
	}

	// Base class overrides - implementation details

	@Override
	protected Delimiter skipWhitespace() throws IOException {

		boolean moreWhitespace;
		do {
			while (charTypeOf(peekChar()) == CharType.whitespace) {
				nextChar();
			}
			
			moreWhitespace = false;

			if ((endLineCommentDelimiter != null) && (charTypeOf(peekChar()) == CharType.delimiter)) {

				Delimiter delimiter = parseDelimiter((char)nextChar());

				if (delimiter.toString() != endLineCommentDelimiter) {
					return delimiter;
				}
				
				int commentLineNumber = lineno();
				while ((moreWhitespace = (peekChar() != eofChar)) && (lineno() == commentLineNumber)) {
					nextChar();
				}
			}
		} while (moreWhitespace);
		
		return null;
	}

	@Override
	protected Token parseWord() throws IOException {

		StringBuilder image = new StringBuilder();
		while (hasNextWordChar())  {
			image.append((char)nextChar());
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(image);
		}

		return new Word(image.toString());
	}

	@Override
	protected Delimiter parseDelimiter(char leader) throws IOException {

		if (compoundDelimiters.isEmpty()) {
			return new Delimiter(String.valueOf(leader));
		}

		String imageSoFar = String.valueOf(leader);
		List<String> potentialMatches = new LinkedList<String>(compoundDelimiters);
		
		do {
			int intChar = nextChar();
			String image = imageSoFar + String.valueOf((char)intChar);
			
			for (ListIterator<String> delimiterIterator = potentialMatches.listIterator(); delimiterIterator.hasNext(); ) {
				String delimiter = delimiterIterator.next();

				if (delimiter.equals(image)) {
					return new Delimiter(delimiter);
				}
				else if (!delimiter.startsWith(image)) {
					delimiterIterator.remove();
				}
			}

			if (potentialMatches.isEmpty()) {
				pushChar(intChar);
				return new Delimiter(imageSoFar);
			}
			
			imageSoFar = image;
		} while (true);
	}

	// Logical character scanning
	
	private boolean hasNextWordChar() throws IOException {
		CharType type = charTypeOf(peekChar());
		return (type == CharType.alphabetic) || (type == CharType.numeric);
	}

}
