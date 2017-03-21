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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class XlsxTargetSheet extends XlsxSheet {

	private Sheet sheet;
	private int rowIndex;
	private Row row;

	public XlsxTargetSheet(Book owner, String name) {
		super(owner, name);

		sheet = null;
		rowIndex = 0;
		row = null;
	}

	// Node overrides

	@Override
	public void create() throws IOException {

		sheet = ((XlsxTargetBook)owner).getBook().createSheet(getName());

		// The following is duplicated in DsvFile.create() and should probably be moved to common code
		// but TxtFile has a different implementation.

		TargetHeaders headers = getTargetHeaders();
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

		String cellImage = null;
		if (object == null) {
			// Leave cell empty
		}
		else if (object instanceof Number) {
			cell.setCellValue(((Number)object).doubleValue());
		}
		else if (object instanceof String) {
			cellImage = (String)object;
			cell.setCellValue(cellImage);
		}
		else if (object instanceof Boolean) {
			cell.setCellValue((Boolean)object);
		}
		else if (object instanceof Date || object instanceof LocalDateTime) {

			Date date;
			LocalTime time;

			if (object instanceof Date) {
				date = (Date)object;
				Instant instant = Instant.ofEpochMilli(date.getTime());
				time = LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).toLocalTime();
			}
			else {
				LocalDateTime dateTime = (LocalDateTime)object;
				date = Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
				time = dateTime.toLocalTime();
			}

			cell.setCellValue(date);

			CellStyle style = null;
			if (time.equals(LocalTime.MIDNIGHT)) {
				style = ((XlsxTargetBook)owner).getDateStyle();
			}
			else {
				style = ((XlsxTargetBook)owner).getDatetimeStyle();
			}
			cell.setCellStyle(style);

			cellImage = style.getDataFormatString();
		}
		else {
			cellImage = object.toString();
			cell.setCellValue(cellImage);
		}

		// Extend column width to hold the widest string encountered.

		if (cellImage != null) {
			final int excelColumnPaddingWidthInCharacters = 2;
			final int excelMaxColumnWidthInCharacters = 255;
			final int excelColumnWidthUnitsPerCharacter = 256;

			int cellWidth = Math.min(cellImage.length() + excelColumnPaddingWidthInCharacters, excelMaxColumnWidthInCharacters) * excelColumnWidthUnitsPerCharacter;

			Sheet sheet = row.getSheet();
			if (cellWidth > sheet.getColumnWidth(columnIndex - 1)) {
				sheet.setColumnWidth(columnIndex - 1, cellWidth);
			}
		}
	}

	// Never called.
	@Override public void open() {}
	@Override public void load() {}
	@Override public Object readColumn(int columnIndex) { return null; }
	@Override public boolean hasRow() { return false; }
}
