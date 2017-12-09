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
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;

import com.hauldata.util.tokenizer.DsvTokenizer;
import com.hauldata.util.tokenizer.EndOfLine;

/**
 * Delimiter Separated Values file.
 */
public abstract class DsvFile extends TextFile {

	protected char separator;

	protected Writer writer;
	protected DsvTokenizer tokenizer;

	public DsvFile(Owner owner, Path path, char separator, PageOptions options) {

		super(owner, path, options);
		this.separator = separator;

		writer = null;
		tokenizer = null;
	}

	protected static class SourceOptions implements PageOptions {

		public static final SourceOptions DEFAULT = new SourceOptions();

		protected boolean raw = false;

		public boolean isRaw() {
			return raw;
		}
	}

	protected SourceOptions getSourceOptions() {
		return getOptions() != null ? (SourceOptions)getOptions() : SourceOptions.DEFAULT;
	}

	// Node overrides

	/**
	 * Create a DSV file with the specified column headers.
	 */
	@Override
	public void create() throws IOException {
	
		writer = getWriter(false);

		TargetHeaders headers = getTargetHeaders();
		if (headers.exist() && !headers.fromMetadata()) {
			for (int columnIndex = 1; columnIndex <= headers.getColumnCount(); ++columnIndex) {
				writeColumn(columnIndex, headers.getCaption(columnIndex - 1));
			}
		}
	}

	/**
	 * Open a DSV file to append content.
	 */
	@Override
	public void append() throws IOException {
	
		writer = getWriter(true);
	}

	/**
	 * Open a DSV file and confirm the column headers match those specified on the constructor.
	 */
	@Override
	public void open() throws IOException {

		Reader reader = getReader();

		tokenizer = new DsvTokenizer(reader, separator, !getSourceOptions().isRaw());

		SourceHeaders headers = getSourceHeaders();
		if (headers.exist()) {
			if (headers.mustValidate()) {

				int columnIndex = 1;
				for (String caption : headers.getCaptions()) {
					Object value = readColumn(columnIndex++);
					String captionFound = (value == null) ? "" : value.toString();
					if (!captionFound.equals(caption)) {
						throw new RuntimeException("Expected column header '" + caption + "', found '" + captionFound + "'");
					}
				}
	
				if (readColumn(columnIndex) != EndOfLine.value) {
					throw new RuntimeException("File has more column headers than specified");
				}
			}
			else {
				ArrayList<String> captions = new ArrayList<String>(); 

				int columnIndex = 1;
				for (Object value = null; (value = readColumn(columnIndex)) != EndOfLine.value; ++columnIndex) {
					captions.add((value == null) ? "" : value.toString());
				}

				if (captions.size() == 0) {
					throw new RuntimeException("File was specified with column headers but header row is blank");
				}

				headers.setCaptions(captions);
			}
		}
	}

	/**
	 * Prepare to read from a DSV file that has already been opened with open().
	 */
	@Override
	public void load() throws IOException {
		// No action is needed.
	}

	@Override
	public void close() throws IOException {
		
		if (writer != null) writer.close();
		if (tokenizer != null) tokenizer.close();
	}

	// PageNode overrides

	@Override
	public boolean hasRow() throws IOException {
		return tokenizer.hasNext();
	}
}
