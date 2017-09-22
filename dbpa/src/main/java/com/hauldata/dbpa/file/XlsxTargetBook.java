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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

public class XlsxTargetBook extends XlsxBook {

	private FileOutputStream out;
	private SXSSFWorkbook book;

	public enum XlsxCellStyle { date, datetime, integer, money };

	private CellStyle[] cellStyles;
	private final String dateFormatString = "mm/dd/yyyy";
	private final String datetimeFormatString = "mm/dd/yyyy hh:mm:ss AM/PM";
	private final String integerFormatString = "0";
	private final String moneyFormatString = "0.00";

	public XlsxTargetBook(Owner owner, Path path) {
		super(owner, path);

		out = null;
		book = null;
	}

	public SXSSFWorkbook getBook() {
		return book;
	}

	public CellStyle getCellStyle(XlsxCellStyle style) {
		return cellStyles[style.ordinal()];
	}

	// Node overrides

	@Override
	public void create() throws IOException {

		out = new FileOutputStream(getName());
		book = new SXSSFWorkbook();

		cellStyles = new CellStyle[XlsxCellStyle.values().length];
		createCellStyle(XlsxCellStyle.date, dateFormatString);
		createCellStyle(XlsxCellStyle.datetime, datetimeFormatString);
		createCellStyle(XlsxCellStyle.integer, integerFormatString);
		createCellStyle(XlsxCellStyle.money, moneyFormatString);
	}

	private void createCellStyle(XlsxCellStyle style, String formatString) {

		int index = style.ordinal();
		cellStyles[index] = book.createCellStyle();
		cellStyles[index].setDataFormat(book.createDataFormat().getFormat(formatString));
	}

	@Override
	public void append() throws IOException {
		// This will be called before adding a new sheet to the book.
		// No action is required at the book level.
		// If this is followed by an attempt to append to an existing sheet in a book
		// not currently open for writing that attempt will fail at the sheet level.
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

	// Never called.
	@Override public void open() {}
	@Override public void load() {}
}
