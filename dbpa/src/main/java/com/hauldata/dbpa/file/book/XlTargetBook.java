/*
 * Copyright (c) 2016-2017, 2020, Ronald DeSantis
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

package com.hauldata.dbpa.file.book;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFColor;

public class XlTargetBook extends XlBook {

	private WorkbookFactory factory;
	private FileOutputStream out;
	private WorkbookWrapper bookWrapper;

	Map<StylesWithFormatting, CellStyle> stylesUsed;
	Map<FontStyles, Font> fontsUsed;
	Map<Integer, XSSFColor> colorsUsed;

	public enum XlsxCellStyle {
		DATE,
		DATETIME,
		INTEGER,
		BIG_INTEGER,
		TWO_DECIMAL,
		BIG_TWO_DECIMAL,
		TWO_DECIMAL_ONLY,
		FOUR_DECIMAL,
		BIG_FOUR_DECIMAL,
		FOUR_DECIMAL_ONLY,
		OTHER_DECIMAL,
		BIG_OTHER_DECIMAL,
		OTHER_DECIMAL_ONLY
	};

	private CellStyle[] cellStyles;
	private final String dateFormatString = "mm/dd/yyyy";
	private final String datetimeFormatString = "mm/dd/yyyy hh:mm:ss AM/PM";
	private final String integerFormatString = "0";
	private final String bigIntegerFormatString = "#,##0";
	private final String twoDecimalFormatString = "0.00";
	private final String bigTwoDecimalFormatString = "#,##0.00";
	private final String twoDecimalOnlyFormatString = ".00";
	private final String fourDecimalFormatString = "0.0000";
	private final String bigFourDecimalFormatString = "#,##0.0000";
	private final String fourDecimalOnlyFormatString = ".0000";
	private final String otherDecimalFormatString = "0.0#########";
	private final String bigOtherDecimalFormatString = "#,##0.0#########";
	private final String otherDecimalOnlyFormatString = ".0#########";

	public XlTargetBook(String typeName, String sheetTypeName, WorkbookFactory factory, Owner owner, Path path) {
		super(typeName, owner, path);
		this.factory = factory;

		out = null;
		bookWrapper = null;

		stylesUsed = new HashMap<StylesWithFormatting, CellStyle>();
		fontsUsed = new HashMap<FontStyles, Font>();
		colorsUsed = new HashMap<Integer, XSSFColor>();
	}

	public Workbook getBook() {
		return bookWrapper.getBook();
	}

	public CellStyle getCellStyle(XlsxCellStyle style) {
		return cellStyles[style.ordinal()];
	}

	// Node overrides

	@Override
	public void create() throws IOException {

		out = new FileOutputStream(getName());
		bookWrapper = factory.newTargetBook();

		cellStyles = new CellStyle[XlsxCellStyle.values().length];
		createCellStyle(XlsxCellStyle.DATE, dateFormatString);
		createCellStyle(XlsxCellStyle.DATETIME, datetimeFormatString);
		createCellStyle(XlsxCellStyle.INTEGER, integerFormatString);
		createCellStyle(XlsxCellStyle.BIG_INTEGER, bigIntegerFormatString);
		createCellStyle(XlsxCellStyle.TWO_DECIMAL, twoDecimalFormatString);
		createCellStyle(XlsxCellStyle.BIG_TWO_DECIMAL, bigTwoDecimalFormatString);
		createCellStyle(XlsxCellStyle.TWO_DECIMAL_ONLY, twoDecimalOnlyFormatString);
		createCellStyle(XlsxCellStyle.FOUR_DECIMAL, fourDecimalFormatString);
		createCellStyle(XlsxCellStyle.BIG_FOUR_DECIMAL, bigFourDecimalFormatString);
		createCellStyle(XlsxCellStyle.FOUR_DECIMAL_ONLY, fourDecimalOnlyFormatString);
		createCellStyle(XlsxCellStyle.OTHER_DECIMAL, otherDecimalFormatString);
		createCellStyle(XlsxCellStyle.BIG_OTHER_DECIMAL, bigOtherDecimalFormatString);
		createCellStyle(XlsxCellStyle.OTHER_DECIMAL_ONLY, otherDecimalOnlyFormatString);
	}

	private void createCellStyle(XlsxCellStyle style, String formatString) {

		int index = style.ordinal();
		cellStyles[index] = bookWrapper.getBook().createCellStyle();
		cellStyles[index].setDataFormat(bookWrapper.getBook().createDataFormat().getFormat(formatString));
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

		bookWrapper.getBook().write(out);
		out.close();
		bookWrapper.close();
	}

	// Never called.
	@Override public void open() {}
	@Override public void load() {}
}
