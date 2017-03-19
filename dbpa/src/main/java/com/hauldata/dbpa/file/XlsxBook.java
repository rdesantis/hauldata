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

package com.hauldata.dbpa.file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

/**
 * Microsoft Excel XLSX workbook
 */
public class XlsxBook extends Book {

	private static final String typeName = "XLSX file";
	static public String typeName() { return typeName; }
	
	public static void registerHandler(String name) {
		Book.Factory bookFactory = new Book.Factory() {
			public Book instantiate(Node.Owner owner, Object path) { return new XlsxBook((File.Owner)owner, (Path)path); }
			public String getTypeName() { return typeName; }
		};
		Sheet.Factory sheetFactory = new Sheet.Factory() {
			public Sheet instantiate(Node.Owner book, Object name) { return new XlsxSheet((Book)book, (String)name); }
			public String getTypeName() { return XlsxSheet.typeName(); }
		};
		FileHandler.register(name, true, new TargetSheetPage.Factory(bookFactory, sheetFactory), null);
	}

	private FileOutputStream out;
	private SXSSFWorkbook book;

	private CellStyle dateStyle;
	private CellStyle datetimeStyle;

	private final String dateFormatString = "mm/dd/yyyy";
	private final String datetimeFormatString = "mm/dd/yyyy hh:mm:ss AM/PM";

	public XlsxBook(Owner owner, Path path) {
		super(owner, path);

		out = null;
		book = null;
	}

	public SXSSFWorkbook getBook() {
		return book;
	}

	public CellStyle getDateStyle() {
		return dateStyle;
	}

	public CellStyle getDatetimeStyle() {
		return datetimeStyle;
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}

	@Override
	public void create() throws IOException {

		out = new FileOutputStream(getName());
		book = new SXSSFWorkbook();

		dateStyle = book.createCellStyle();
		dateStyle.setDataFormat(book.createDataFormat().getFormat(dateFormatString));

		datetimeStyle = book.createCellStyle();
		datetimeStyle.setDataFormat(book.createDataFormat().getFormat(datetimeFormatString));
	}

	@Override
	public void append() throws IOException {
		// This will be called before adding a new sheet to the book.
		// No action is required at the book level.
		// If this is followed by an attempt to append to an existing sheet in a book
		// not currently open for writing that attempt will fail at the sheet level.
	}

	@Override
	public void open() throws IOException {
		throw new RuntimeException("Opening XLSX files for reading is not supported: " + getName());
	}

	@Override
	public void load() throws IOException {
		throw new RuntimeException("Opening XLSX files for loading is not supported: " + getName());
	}

	@Override
	public void close() throws IOException {

		// Close all sheets.

		for (Sheet sheet : sheets.values()) {
			sheet.close();
		}

		// Close the book.

		book.write(out);
		out.close();
		book.dispose();
	}
}
