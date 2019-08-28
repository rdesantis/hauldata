/*
 * Copyright (c) 2016-2017, 2019, Ronald DeSantis
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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.util.tokenizer.EndOfLine;

public class XlsxSourceSheet extends XlsxSheet {

	private Sheet sheet;
	private int rowIndex;
	private Row row;

	public XlsxSourceSheet(Book owner, String name, PageOptions options) {
		super(owner, name, options);

		sheet = null;
		rowIndex = 0;
		row = null;
	}

	@Override
	public void open() throws IOException {

		sheet = ((XlsxSourceBook)owner).getBook().getSheet(getName());
		
		// The following is duplicated in DsvFile.open() and should probably be moved to common code.

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
	 * Prepare to read from a XLSX sheet that has already been opened with open().
	 */
	@Override
	public void load() throws IOException {
		// No action is needed.
	}

	@Override
	public Object readColumn(int columnIndex) throws IOException {

		try {
			if (columnIndex == 1) {
				row = sheet.getRow(rowIndex++);
			}

			if (row == null) {
				// There are no non-empty cells in the row.

				if (headers.getColumnCount() == 0) {
					throw new RuntimeException("Row is empty and numbers of columns is not established; cannot read");
				}
				else if (columnIndex <= headers.getColumnCount()) {
					return null;
				}
				else {
					return EndOfLine.value;
				}
			}
			else if (row.getLastCellNum() < columnIndex) {
				// There is no cell at this column index or after.

				if (headers.getColumnCount() == 0) {
					return EndOfLine.value;
				}
				else if (columnIndex <= headers.getColumnCount()) {
					return null;
				}
				else {
					return EndOfLine.value;
				}
			}
			else {
				// There are non-empty cells in the row at least up to this one.

				Object value = fromXLSX(row.getCell(columnIndex - 1));

				if (headers.getColumnCount() == 0) {
					return value;
				}
				else if (columnIndex <= headers.getColumnCount() || rowIndex == 1) {
					return value;
				}
				else {
					return EndOfLine.value;
				}
			}
		}
		catch (Exception ex) {
			throw new RuntimeException("At row " + Integer.toString(rowIndex) + ": " + ex.getMessage(), ex);
		}
	}

	private Object fromXLSX(Cell cell) {
		if (cell == null) {
			return null;
		}

		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
			return null;
		case Cell.CELL_TYPE_BOOLEAN:
			return cell.getBooleanCellValue();
		case Cell.CELL_TYPE_NUMERIC:
			double numericValue = cell.getNumericCellValue();
			return DateUtil.isCellDateFormatted(cell) ? DateUtil.getJavaDate(numericValue) : (Double)numericValue;
		case Cell.CELL_TYPE_STRING:
		default:
			return cell.getStringCellValue();
		}
	}

	@Override
	public boolean hasRow() {
		return rowIndex <= sheet.getLastRowNum();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	// Never called.
	@Override public void create() throws IOException {}
	@Override public void append() throws IOException {}
	@Override public void writeColumn(int columnIndex, Object object) {}
	@Override public void flush() {}
}
