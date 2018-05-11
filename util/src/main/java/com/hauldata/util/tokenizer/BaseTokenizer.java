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

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.InputMismatchException;
import java.util.regex.Pattern;

public abstract class BaseTokenizer {

	private Reader reader;

	public enum CharType { unknown, whitespace, alphabetic, numeric, quote, delimiter, endOfLine };

	final static char cr = '\r';
	final static char lf = '\n';

	private final static char decimalPoint = '.';
	private final static char minusSign = '-';
	private final static char exponential = 'E';

	final static int maxSupportedCharCode = 255;
	private CharType[] charType = new CharType[maxSupportedCharCode + 1];

	public enum EscapeType { doubleQuote, json };

	private boolean eolIsSignificant;
	private boolean splitQuoteIsAllowed;
	private boolean negativeIsRespected;
	private EscapeType escapeType;

	private int lookaheadChar;
	private Token lookaheadToken;
	private Token lookbackToken;
	private int lineNumber;

	protected BaseTokenizer(Reader reader, boolean isNumericRecognized) {

		this.reader = reader;

		setCharType(CharType.unknown, 0, maxSupportedCharCode);

		if (isNumericRecognized) {
			setCharType(CharType.numeric, (int)'0', (int)'9');
		}

		charType[(int)cr] = CharType.endOfLine;
		charType[(int)lf] = CharType.endOfLine;

		eolIsSignificant = true;
		splitQuoteIsAllowed = true;
		negativeIsRespected = false;
		escapeType = EscapeType.doubleQuote;

		lineNumber = 1;

		lookaheadChar = noChar;
		lookaheadToken = null;
		lookbackToken = null;
	}

	protected BaseTokenizer(Reader reader) {
		this(reader, true);
	}

	/**
     * Sets the type of a range of characters.
     *
     * @return	this tokenizer
	 */
	public BaseTokenizer setCharType(CharType charType, int low, int hi) {
		for (int i = low; i <= hi; ++i) {
			this.charType[i] = charType;
		}
		return this;
	}

	/**
     * Sets the type of a single character.
     *
     * @return	this tokenizer
	 */
	public BaseTokenizer setCharType(CharType charType, char ch) {
		this.charType[(int)ch] = charType;
		return this;
	}

	protected CharType getCharType(char ch) {
		return charType[(int)ch];
	}

    /**
     * Determines whether or not ends of line are treated as tokens.
     * If the flag argument is true, this tokenizer treats end of lines
     * as tokens; the <code>nextToken</code> method returns an
     * <code>EndOfLine</code> object.
     * <p>
     * If the <code>flag</code> is false, end-of-line characters are
     * treated as white space and serve only to separate tokens.
     *
     * @param	flag	<code>true</code> indicates that end-of-line characters
     *					are separate tokens; <code>false</code> indicates that
     *					end-of-line characters are white space.
     *
     * @return	this tokenizer
     * @see		java.io.StreamTokenizer#eolIsSignificant(boolean)
	 */
	public BaseTokenizer eolIsSignificant(boolean flag) {
        eolIsSignificant = flag;
		return this;
    }

	/**
     * Determines whether or not quoted strings may be split across lines.
     * If the flag argument is true, this tokenizer allows an opening quote
     * to appear on one line and the closing quote to appear on a subsequent
     * line.  The quoted string token includes any carriage return or
     * line break characters contained within the quotes.
     * <p>
     * If the <code>flag</code> is false, an opening quote must be closed
     * on the same line.  If not, the token is terminated at end of line
     * and is returned as Unknown type instead of Quoted.
     *
     * This must only be used in conjunction with <code>eolIsSignificant(true)</code>;
     * otherwise, results are unpredictable.
     *
     * @param	flag	<code>true</code> indicates that quoted string
     *					may be split across lines; <code>false</code>
     *					indicates that quoted string must be contained
     *					on a single line.
     *
     * @return	this tokenizer
     * @see		java.io.StreamTokenizer#eolIsSignificant(boolean)
	 */
	public BaseTokenizer splitQuoteIsAllowed(boolean flag) {
		splitQuoteIsAllowed = flag;
		return this;
	}

    /**
     * Determines whether or not the negative sign is considered
     * to be part of a numeric token.
     * If the flag argument is true, this tokenizer treats a leading negative
     * sign as part of a Numeric<Type> token; the object returned by
     * <code>getValue()</code> has a negative value.
     * <p>
     * If the <code>flag</code> is false, a leading negative sign is
     * treated as as a separate delimiter token.
     *
     * @param	flag	<code>true</code> indicates that a leading negative
     *					sign is part of a numeric token; <code>false</code>
     *					indicates that leading negative sign is a distinct
     *					delimiter token.
     *
     * @return	this tokenizer
     * @see		java.io.StreamTokenizer#eolIsSignificant(boolean)
	 */
	public BaseTokenizer negativeIsRespected(boolean flag) {
		negativeIsRespected = flag;
		return this;
    }

	/**
	 * Determines how to escape special characters within quoted string.
	 * <p>
	 * @param type	<code>EscapeType.doubleQuote</code> indicates that
	 *				the quote character is escaped by preceding it with
	 *				another quote character;
	 *
	 *				<code>EscapeType.json</code> indicates that backslash
	 *				is the escape character and JSON escape sequences are
	 *				recognized; see https://www.json.org/
	 *
     * @return	this tokenizer
	 */
	public BaseTokenizer escape(EscapeType type) {
		escapeType = type;
		return this;
	}

	/**
     * Returns true if this tokenizer has another token in its input.
     * This method may block while waiting for input to scan.
     * The tokenizer does not advance past any input.
     *
     * @return	true if and only if this tokenizer has another token
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
    public boolean hasNext() throws IOException {
		return (peekToken() != null);
	}

    /**
     * Returns true if this tokenizer has another token on the
     * current line.  This must only be used in conjunction with
     * <code>eolIsSignificant(true)</code>; otherwise, results are
     * unpredictable.
     *
     * This method may block while waiting for input to scan.
     * The tokenizer does not advance past any input.
     *
     * @return	true if and only if this tokenizer has another token
     * 			on the current line.
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
    public boolean hasNextOnLine() throws IOException {
    	Object token = peekToken();
		return ((token != null) && !(token instanceof EndOfLine));
	}

    /**
     * Returns true if the next token in this tokenizer's input is a
     * non-whitespace delimiter.  The tokenizer does not advance past any input.
     *
     * @return	true if and only if this tokenizer's next token is a
     *			non-whitespace delimiter
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	public boolean hasNextDelimiter() throws IOException {
		return (peekToken() instanceof Delimiter);
	}

    /**
     * Scans the next token from the input stream of this tokenizer
     * and returns its value as an object or null if no tokens remain.
     * The following object types may be returned:
     * <ul>
     * <li><code>Numeric&lt;Integer&gt;</code> - the token consists of all numeric digits within the range of Integer
     * <li><code>Numeric&lt;Long&gt;</code> - the token consists of all numeric digits beyond the range of Integer but within the range of Long
     * <li><code>Numeric&lt;BigInteger&gt;</code> - the token consists of all numeric digits beyond the range of Long
     * <li><code>Numeric&lt;BigDecimal&gt;</code> - the token consists of numeric digits with one leading or embedded decimal point
     * <li><code>Word</code> - the token is an identifier consisting of word characters starting with an alphabetic character
     * <li><code>Quoted</code> - the token is quoted text
     * <li><code>Delimiter</code> - the token is a non-whitespace delimiter
     * <li><code>EndOfLine</code> - the token is end of line
     * <li><code>Unknown</code> - the token contains an unrecognized combination of characters
     * <li><code>null</code> - no tokens remain in the input stream
     * </ul>
     * <p>
     * Typical clients of this
     * class first set up the syntax tables and then sit in a loop
     * calling nextToken to parse successive tokens until null
     * is returned.
     *
     * @return	an object representing the value of the scanned token or null
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this scanner.
     * @throws	InputMismatchException if the next token is not a recognized type
     * @see		BaseTokenizer#eolIsSignificant(boolean)
     * @see		BaseTokenizer#useDelimiter(Pattern)
     * @see		BaseTokenizer#useDelimiter(String)
     */
	public Token nextToken() throws IOException, InputMismatchException {
		Token token = peekToken();
		lookaheadToken = null;
		return rememberToken(token);
	}

    /**
     * Scans the next token from the current line of the input stream
     * and returns its value as an object or null if no tokens remain
     * on the line.  This must only be used in conjunction with
     * <code>eolIsSignificant(true)</code>; otherwise, results are
     * unpredictable.
     * <p>
     * The last line of a text file is not guaranteed to be terminated
     * with an end-of-line character sequence.  It instead may be
     * terminated by end of file.  This method makes it unnecessary to
     * test for both end of line and end of file.
     * <p>
     * If end of line is detected by this method, it is discarded.
     * If end of file is detected, it is effectively pushed back
     * so it will be detected by a subsequent call to
     * <code>hasNext()</code> or <code>nextToken()</code>.
     * <p>
     * This method cannot be used by the caller to detect end of file.
     * Another method must be used.
     *
     * @return	an object representing the value of the scanned token or
     *			null if end of line or end of file is found
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Read</code> object of this scanner.
     * @throws	InputMismatchException if the next token is not a recognized type
     * @see		BaseTokenizer#eolIsSignificant(boolean)
     * @see		BaseTokenizer#nextToken()
     * @see		BaseTokenizer#hasNext()
     */
	public Token nextTokenOnLine() throws IOException, InputMismatchException {
		Token token = peekToken();
		if (token != null) {
			nextToken();
			if (token instanceof EndOfLine) {
				token = null;
			}
		}
		return rememberToken(token);
	}

	protected Token rememberToken(Token scannedToken) {
		lookbackToken = scannedToken;
		return scannedToken;
	}

	/**
	 * @return the last token scanned
	 */
	public Token lastToken() {
		return lookbackToken;
	}

	/**
     * @see		java.io.StreamTokenizer#lineno()
	 */
	public int lineno() {
		return lineNumber;
	}

	/**
	 * Closes the tokenizer and releases any system resources associated with it
	 * including the underlying Reader.
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		reader.close();
	}

	// Physical token scanning

	protected Token peekToken() throws IOException, InputMismatchException {
		if (lookaheadToken == null) {
			lookaheadToken = getToken();
		}
		return lookaheadToken;
	}

	protected Token getToken() throws IOException {

		Token token = skipWhitespace();
		if (!(token instanceof Unknown)) {
			return token;
		}

		int intChar = nextChar();

		if (intChar == eofChar) {
			return null;
		}

		if (negativeIsRespected && ((char)intChar == minusSign)) {
			if (hasNextNumericChar()) {
				return parseInteger(token.hasLeadingWhitespace(), new StringBuilder(String.valueOf(minusSign)), true);
			}
			else if ((char)peekChar() == decimalPoint) {
				return parseDecimal(token.hasLeadingWhitespace(), new StringBuilder(String.valueOf(minusSign)), BigDecimal.ZERO, true);
			}
		}
		else if ((char)intChar == decimalPoint) {
			pushChar(intChar);
			return parseDecimal(token.hasLeadingWhitespace(), new StringBuilder(), BigDecimal.ZERO, false);
		}

		switch (charTypeOf(intChar)) {
		case delimiter:
			return parseDelimiter(token.hasLeadingWhitespace(), (char)intChar);
		case alphabetic:
			pushChar(intChar);
			return parseWord(token.hasLeadingWhitespace());
		case numeric:
			pushChar(intChar);
			return parseInteger(token.hasLeadingWhitespace(), new StringBuilder(), false);
		case quote:
			return parseQuoted(token.hasLeadingWhitespace(), (char)intChar);
		case endOfLine:
			return parseEndOfLine((char)intChar);
		default:
		case unknown:
			StringBuilder image = new StringBuilder(String.valueOf((char)intChar));
			return parseUnknown(token.hasLeadingWhitespace(), image);
		}
	}

	/**
	 * Skips whitespace characters in the input stream.
	 * @return an instance of Delimiter parsed from the input stream if the next token is a delimiter token,
	 * otherwise an instance of Unknown.  In the latter case, no token is actually parsed from the input stream
	 * but the hasLeadingWhitespace() member tells if whitespace was actually skipped.
	 * @throws IOException
	 */
	protected abstract Token skipWhitespace() throws IOException;

	protected abstract Delimiter parseDelimiter(boolean leadingWhitespace, char leader) throws IOException;

	protected abstract Token parseWord(boolean leadingWhitespace) throws IOException;

	private Token parseInteger(boolean leadingWhitespace, StringBuilder image, boolean negate) throws IOException {

		final int maxDiv10 = Integer.MAX_VALUE / 10;
		final int maxMod10 = Integer.MAX_VALUE - (maxDiv10 * 10);

		int value = 0;
		while (hasNextNumericChar())  {

			int intChar = nextChar();

			int digit = intChar - (int)'0';

			if ((value > maxDiv10) || ((value == maxDiv10) && (digit >= maxMod10))) {
				pushChar(intChar);
				return parseLong(leadingWhitespace, image, value, negate);
			}

			image.append((char)intChar);

			value = value * 10 + digit;
		}

		if ((char)peekChar() == decimalPoint) {
			return parseDecimal(leadingWhitespace, image, BigDecimal.valueOf(value), negate);
		}

		if ((char)peekChar() == exponential) {
			return parseScientific(leadingWhitespace, image, BigDecimal.valueOf(value), negate);
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(leadingWhitespace, image);
		}

		if (negate) {
			value = -value;
		}

		return new Numeric<Integer>(leadingWhitespace, image.toString(), value);
	}

	private Token parseLong(boolean leadingWhitespace, StringBuilder image, int valueSoFar, boolean negate) throws IOException {

		final long maxDiv10 = Long.MAX_VALUE / 10;
		final long maxMod10 = Long.MAX_VALUE - (maxDiv10 * 10);

		long value = valueSoFar;
		while (hasNextNumericChar())  {

			int intChar = nextChar();

			int digit = intChar - (int)'0';

			if ((value > maxDiv10) || ((value == maxDiv10) && (digit >= maxMod10))) {
				pushChar(intChar);
				return parseBigInteger(leadingWhitespace, image, value, negate);
			}

			image.append((char)intChar);

			value = value * 10 + digit;
		}

		if ((char)peekChar() == decimalPoint) {
			return parseDecimal(leadingWhitespace, image, BigDecimal.valueOf(value), negate);
		}

		if ((char)peekChar() == exponential) {
			return parseScientific(leadingWhitespace, image, BigDecimal.valueOf(value), negate);
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(leadingWhitespace, image);
		}

		if (negate) {
			value = -value;
		}

		return new Numeric<Long>(leadingWhitespace, image.toString(), value);
	}

	private Token parseBigInteger(boolean leadingWhitespace, StringBuilder image, long valueSoFar, boolean negate) throws IOException {

		BigInteger value = BigInteger.valueOf(valueSoFar);
		while (hasNextNumericChar())  {

			int intChar = nextChar();

			int digit = intChar - (int)'0';

			image.append((char)intChar);

			value = value.multiply(BigInteger.TEN).add(BigInteger.valueOf(digit));
		}

		if ((char)peekChar() == decimalPoint) {
			return parseDecimal(leadingWhitespace, image, new BigDecimal(value), negate);
		}

		if ((char)peekChar() == exponential) {
			return parseScientific(leadingWhitespace, image, new BigDecimal(value), negate);
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(leadingWhitespace, image);
		}

		if (negate) {
			value = value.negate();
		}

		return new Numeric<BigInteger>(leadingWhitespace, image.toString(), value);
	}

	private Token parseDecimal(boolean leadingWhitespace, StringBuilder image, BigDecimal value, boolean negate) throws IOException {

		if ((char)nextChar() != decimalPoint) {
			throw new InputMismatchException("parseDecimal() requires decimal point to be the next character parsed");
		}

		image.append(decimalPoint);

		int decimalPlaces = 0;
		while (hasNextNumericChar())  {

			++decimalPlaces;

			int intChar = nextChar();

			int digit = intChar - (int)'0';

			image.append((char)intChar);

			value = value.add(BigDecimal.valueOf(digit, decimalPlaces));
		}

		if ((decimalPlaces == 0) || ((char)peekChar() == decimalPoint)) {
			return parseUnknown(leadingWhitespace, image);
		}

		if ((char)peekChar() == exponential) {
			return parseScientific(leadingWhitespace, image, value, negate);
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(leadingWhitespace, image);
		}

		if (negate) {
			value = value.negate();
		}

		return new Numeric<BigDecimal>(leadingWhitespace, image.toString(), value);
	}

	private Token parseScientific(boolean leadingWhitespace, StringBuilder image, BigDecimal value, boolean negate) throws IOException {

		if ((char)nextChar() != exponential) {
			throw new InputMismatchException("parseScientific() requires \"" + String.valueOf(exponential) + "\" to be the next character parsed");
		}

		image.append(exponential);

		boolean negateExponent = false;
		if ((char)peekChar() == minusSign) {
			negateExponent = true;
			nextChar();
		}

		if (!hasNextNumericChar()) {
			return parseUnknown(leadingWhitespace, image);
		}

		int exponent = 0;
		while (hasNextNumericChar())  {

			int intChar = nextChar();

			image.append((char)intChar);

			int digit = intChar - (int)'0';

			exponent = exponent * 10 + digit;

			if (exponent > Double.MAX_EXPONENT) {
				return parseUnknown(leadingWhitespace, image);
			}
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(leadingWhitespace, image);
		}

		if (negateExponent) {
			exponent = -exponent;
		}

		value = value.movePointRight(exponent);

		if (negate) {
			value = value.negate();
		}

		return new Numeric<BigDecimal>(leadingWhitespace, image.toString(), value);
	}

	private Token parseQuoted(boolean leadingWhitespace, char quote) throws IOException {

		StringBuilder image = new StringBuilder();

		boolean isQuoteClosed = false;

		int intChar;
		while (peekChar() != eofChar) {
			intChar = nextChar();

			if ((char)intChar == quote) {
				if ((escapeType == EscapeType.doubleQuote) && ((char)peekChar() == quote)) {
					nextChar();
					image.append(quote);
				}
				else {
					isQuoteClosed = true;
					break;
				}
			}
			else if ((escapeType == EscapeType.json) && ((char)intChar == '\\')) {
				intChar = nextJsonEscapeChar();
				if (intChar == noChar) {
					break;
				}
				else {
					image.append((char)intChar);
				}
			}
			else {
				if (!splitQuoteIsAllowed && (charTypeOf(intChar) == CharType.endOfLine)) {
					pushChar(intChar);
					break;
				}
				else {
					image.append((char)intChar);
				}
			}
		}

		if (!isQuoteClosed) {
			return new Unknown(leadingWhitespace, image.insert(0, quote).toString());
		}

		if (!hasNextTerminatingChar()) {
			return parseUnknown(leadingWhitespace, image.insert(0, quote).append(quote));
		}

		return new Quoted(leadingWhitespace, quote, image.toString());
	}

	private EndOfLine parseEndOfLine(char sentinel) throws IOException {

		if (sentinel == cr) {
			if ((char)peekChar() == lf) {
				nextChar();
			}
		}
		return EndOfLine.value;
	}

	protected Unknown parseUnknown(boolean leadingWhitespace, StringBuilder image) throws IOException {

		while (!hasNextTerminatingChar()) {
			image.append((char)nextChar());
		}
		return new Unknown(leadingWhitespace, image.toString());
	}

	// JSON escape scanning

	private int nextJsonEscapeChar() throws IOException {
		int intChar = nextChar();

		switch (intChar) {
		case 'b': return '\b';
		case 'f': return '\f';
		case 'n': return '\n';
		case 'r': return '\r';
		case 't': return '\t';
		case 'u': return nextJsonHexEscape();
		default: return intChar;
		}
	}

	private int nextJsonHexEscape() throws IOException {

		StringBuilder hex = new StringBuilder();
		for (int i = 0; i < 4; ++i) {
			int intChar = nextChar();
			if (intChar < 0) {
				return noChar;
			}
			char ch = (char)intChar;
			if (Character.digit(ch, 16) == -1) {
				return noChar;
			}
			hex.append(ch);
		}
		int intChar = Integer.parseInt(hex.toString(), 16);
		return (0 <= intChar) ? intChar : noChar;
	}

	// Logical character scanning

	protected boolean hasNextTerminatingChar() throws IOException {
		CharType type = charTypeOf(peekChar());
		return (type == CharType.delimiter) || (type == CharType.whitespace) || (type == CharType.endOfLine);
	}

	protected boolean hasNextNumericChar() throws IOException {
		CharType type = charTypeOf(peekChar());
		return (type == CharType.numeric);
	}

	protected CharType charTypeOf(int intChar) {
		return
				(intChar == eofChar) ? CharType.endOfLine :
				(intChar > maxSupportedCharCode) ? CharType.unknown :
				charType[intChar];
	}

	// Physical character scanning

	protected final int eofChar = -1;
	protected final int noChar = -2;

	protected int peekChar() throws IOException {
		if (lookaheadChar == noChar) {
			lookaheadChar = reader.read();
			if (lookaheadChar == lf) {
				++lineNumber;
			}
			if (!eolIsSignificant && ((lookaheadChar == cr) || (lookaheadChar == lf))) {
				lookaheadChar = (int)' ';
			}
		}
		return lookaheadChar;
	}

	protected int nextChar() throws IOException {
		int result = peekChar();
		lookaheadChar = noChar;
		return result;
	}

	protected void pushChar(int pushedChar) throws IOException {
		if (lookaheadChar != noChar) {
			throw new IOException("invalid pushChar()");
		}
		lookaheadChar = pushedChar;
	}
}
