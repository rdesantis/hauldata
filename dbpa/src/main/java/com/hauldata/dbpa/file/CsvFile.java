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
import java.nio.file.Path;

import com.hauldata.util.tokenizer.Delimiter;
import com.hauldata.util.tokenizer.EndOfLine;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Token;
import com.hauldata.util.tokenizer.Unknown;

/**
 * Common Separated Values file.
 */
public class CsvFile extends DsvFile {

	private static final String typeName = "CSV file";
	static public String typeName() { return typeName; }

	public static void registerHandler(String name) {
		File.Factory fileFactory = new File.Factory() {
			public File instantiate(Node.Owner owner, Object path, File.Options options) { return new CsvFile((File.Owner)owner, (Path)path, options); }
			public String getTypeName() { return typeName; }
		};
		FileHandler.register(name, false, new TargetFilePage.Factory(fileFactory), new TargetOptions.Factory(), new SourceFilePage.Factory(fileFactory), null);
	}

	public CsvFile(Owner owner, Path path, Options options) {
		super(owner, path, ',', options);
	}

	private static class TargetOptions implements File.Options {

		public static final TargetOptions DEFAULT = new TargetOptions();

		boolean noQuotes = false;

		@Override
		public boolean set(String name) {
			if (name.equals("NOQUOTES")) {
				noQuotes = true;
				return true;
			}
			return false;
		}

		public boolean isNoQuotes() {
			return noQuotes;
		}

		public static class Factory implements File.Options.Factory {
			@Override
			public Options make() {
				return new TargetOptions();
			}
		}
	}

	private TargetOptions getTargetOptions() {
		// TODO: This function should just be the (TargetOptions)getOptions() cast.
		// But due to undocumented APPEND without prior CREATE, getOptions() may return null.
		return getOptions() != null ? (TargetOptions)getOptions() : TargetOptions.DEFAULT;
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}

	// PageNode overrides

	private final String quote = "\"";
	private final String quote_quote = quote + quote;

	public void writeColumn(int columnIndex, Object object) throws IOException {

		
		if (1 < columnIndex) {
			writer.write(separator);
		}

		if (object != null) {
			String value = object.toString();
			if (object instanceof String || object instanceof Character) {
				if (mustQuote(value)) {
					if (value.contains(quote)) {
						value = value.replace(quote, quote_quote);
					}
					value = quote + value + quote;
				}
			}
			writer.write(value);
		}

		if (columnIndex == headers.getColumnCount()) {
			writer.write(String.format("%n"));
		}
	}

	private boolean mustQuote(String value) {
		return value.contains(quote) || 0 <= value.indexOf(separator) || !getTargetOptions().isNoQuotes();
	}

	public Object readColumn(int columnIndex) throws IOException {

		try {
			if (1 < columnIndex) {
				// Discard the separator after the previous token.

				Token terminator = tokenizer.nextTokenOnLine();
				if (terminator == null) {
					if (headers.getColumnCount() == 0) {
						headers.setColumnCount(--columnIndex);
					}
					else if (columnIndex <= headers.getColumnCount()) {
						throw new RuntimeException("Too few columns on line");
					}
					return EndOfLine.value;
				}
				else if (!(terminator instanceof Delimiter) || !(terminator.toString().equals(String.valueOf(separator)))) {
					throw new RuntimeException("Column separator \"" + separator + "\" not found where expected");
				}
			}

			if (tokenizer.hasNextOnLine()) {
				if ((headers.getColumnCount() != 0) && (headers.getColumnCount() < columnIndex)) {
					throw new RuntimeException("Too many columns on line");
				}

				if (tokenizer.hasNextDelimiter()) {
					return null;
				}

				Token token = tokenizer.nextToken();

				if (token instanceof Quoted) {
					return ((Quoted)token).getBody();
				}
				else if (token instanceof Unknown) {
					if (token.toString().equals("true")) {
						return new Boolean(true);
					}
					else if (token.toString().equals("false")) {
						return new Boolean(false);
					}
				}
				else if (token instanceof Delimiter) {
					throw new RuntimeException("Unexpected delimiter \"" + token.toString() + "\"on line");
				}

				return token.getValue();
			}
			else {
				if (columnIndex < headers.getColumnCount()){
					throw new RuntimeException("Too few columns on line");
				}
				return null;
			}
		}
		catch (Exception ex) {
			throw new RuntimeException("At line " + Integer.toString(tokenizer.lineno()) + ": " + ex.getMessage(), ex);
		}
	}
}
