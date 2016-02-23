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

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

/**
 * Microsoft Excel XLSX worksheet
 */
public class XlsxSheet extends com.hauldata.dbpa.file.Sheet {

	static final String typeName = "XLSX sheet";
	static public String typeName() { return typeName; }

	private Sheet sheet;
	private int rowIndex;
	private Row row;

	public XlsxSheet(Book owner, String name) {
		super(owner, name);

		sheet = null;
		rowIndex = 0;
		row = null;
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}

	@Override
	public void create() throws IOException {

		sheet = ((XlsxBook)owner).getBook().createSheet(getName());

		// The following is duplicated in DsvFile.create() and should probably be moved to common code
		// but TxtFile has a different implementation.

		WriteHeaders headers = getWriteHeaders();
		if (headers.exist() && !headers.fromMetadata()) {
			for (int columnIndex = 1; columnIndex <= headers.getColumnCount(); ++columnIndex) {
				writeColumn(columnIndex, headers.getCaption(columnIndex - 1));
			}
		}
	}

	@Override
	public void append() throws IOException {
		if (!isOpen() || !isWritable()) {
			throw new RuntimeException("Appending a sheet in an existing XLSX book is not supported: " + getName());
		}
	}

	@Override
	public void open() throws IOException {
		throw new RuntimeException("Opening a sheet in an XLSX book for reading is not supported: " + getName());
	}

	@Override
	public void load() throws IOException {
		throw new RuntimeException("Reading an existing XLSX sheet is not supported: " + getName());
	}

	@Override
	public void close() throws IOException {

		if (headers.exist()) {
			sheet.createFreezePane(0, 1);
		}
	}

	// PageNode overrides

	@Override
	public void writeColumn(int columnIndex, Object object) throws IOException {

		if (columnIndex == 1) {
			row = sheet.createRow(rowIndex++);
		}

		Cell cell = row.createCell(columnIndex - 1);
		
		if (object == null) {
			// Leave cell empty
		}
		else if (object instanceof Number) {
			cell.setCellValue(((Number)object).doubleValue());
		}
		else if (object instanceof String) {
			cell.setCellValue((String)object);
		}
		else if (object instanceof Boolean) {
			cell.setCellValue((Boolean)object);
		}
		else if (object instanceof Date) {
			cell.setCellValue((Date)object);
			cell.setCellStyle(((XlsxBook)owner).getDateStyle());
		}
		else if (object instanceof LocalDateTime) {
			Instant instant = ((LocalDateTime)object).toInstant(ZoneOffset.UTC);
			Date date = Date.from(instant);
			cell.setCellValue(date);
			cell.setCellStyle(((XlsxBook)owner).getDatetimeStyle());
		}
		else {
			cell.setCellValue(object.toString());
		}

		// Extend column width to hold the widest string encountered.

		if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
			final int excelColumnPaddingWidthInCharacters = 2;
			final int excelMaxColumnWidthInCharacters = 255;
			final int excelColumnWidthUnitsPerCharacter = 256;

			int cellWidth = Math.min(cell.getStringCellValue().length() + excelColumnPaddingWidthInCharacters, excelMaxColumnWidthInCharacters) * excelColumnWidthUnitsPerCharacter;

			Sheet sheet = row.getSheet();
			if (cellWidth > sheet.getColumnWidth(columnIndex - 1)) {
				sheet.setColumnWidth(columnIndex - 1, cellWidth);
			}
		}
	}

	@Override
	public Object readColumn(int columnIndex) throws IOException {
		// Reading not supported; this will never be called.
		return null;
	}

	@Override
	public boolean hasRow() throws IOException {
		// Reading not supported; this will never be called.
		return false;
	}
}
