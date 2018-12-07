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

package com.hauldata.dbpa.file.flat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import com.hauldata.dbpa.file.File;
import com.hauldata.dbpa.file.FileHandler;
import com.hauldata.dbpa.file.Node;
import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.util.tokenizer.EndOfLine;

/**
 * Text file with each record a single column
 */
public class TxtFile extends TextFile {

	private static final String typeName = "TXT file";
	static public String typeName() { return typeName; }

	public static void registerHandler(String name) {
		File.Factory fileFactory = new File.Factory() {
			public File instantiate(Node.Owner owner, Object path, PageOptions options) { return new TxtFile((File.Owner)owner, (Path)path, options); }
			public String getTypeName() { return typeName; }
		};
		FileHandler.register(name, false, new TargetFilePage.Factory(fileFactory), new TargetOptions.Parser(), new SourceFilePage.Factory(fileFactory), null);
	}

	private BufferedWriter writer;
	private BufferedReader reader;
	private String lookaheadRow;

	public TxtFile(Owner owner, Path path, PageOptions options) {
		super(owner, path, options);
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}

	/**
	 * Create a TXT file with the specified column header.
	 */
	@Override
	public void create() throws IOException {

		writer = getWriter(false);

		TargetHeaders headers = getTargetHeaders();
		if (headers.exist() && !headers.fromMetadata()) {
			if (headers.getColumnCount() != 1) {
				throw new RuntimeException("TXT files must have exactly one column; wrong number of headers specified");
			}
			writeColumn(1, headers.getCaption(0));
		}
	}

	/**
	 * Open a TXT file to append content.
	 */
	@Override
	public void append() throws IOException {

		writer = getWriter(true);
	}

	/**
	 * Open a TXT file and confirm the column header matches that specified.
	 */
	@Override
	public void open() throws IOException {

		reader = getReader();
		lookaheadRow = reader.readLine();

		SourceHeaders headers = getSourceHeaders();
		if ((headers != null) && headers.exist()) {
			if (headers.mustValidate()) {

				if (headers.getColumnCount() != 1) {
					throw new RuntimeException("TXT files must have exactly one column; wrong number of headers specified");
				}

				String caption = headers.getCaptions().get(0);
				Object value = readColumn(1);
				String captionFound = (value == null) ? "" : value.toString();
				if (!captionFound.equals(caption)) {
					throw new RuntimeException("File column header does not match that specified");
				}

				readColumn(2);
			}
			else {
				ArrayList<String> captions = new ArrayList<String>();

				Object value = readColumn(1);
				String captionFound = (value == null) ? "" : value.toString();
				if (captionFound.isEmpty()) {
					throw new RuntimeException("File was specified with column headers but header row is blank");
				}

				captions.add(captionFound);

				readColumn(2);

				headers.setCaptions(captions);
			}
		}
	}

	@Override
	public void load() throws IOException {
		// No action is needed.
	}

	@Override
	public void close() throws IOException {

		if (writer != null) writer.close();
		if (reader != null) reader.close();
	}

	// PageNode overrides

	@Override
	public void writeColumn(int columnIndex, Object object) throws IOException {

		if (columnIndex != 1) {
			throw new RuntimeException("TXT files must have exactly one column");
		}
		if (object != null) {
			writer.write(object.toString());
		}
		writer.write(getTargetOptions().getEndOfLine());
	}

	@Override
	public Object readColumn(int columnIndex) throws IOException {
		if (columnIndex == 1) {
			return lookaheadRow;
		}
		else if (columnIndex == 2) {
			lookaheadRow = reader.readLine();
			return EndOfLine.value;
		}
		else {
			throw new RuntimeException("Attempting to read past column one of TXT file");
		}
	}

	@Override
	public boolean hasRow() throws IOException {
		return lookaheadRow != null;
	}
}
