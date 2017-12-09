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

package com.hauldata.dbpa.file.book;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class XlsxSourceBook extends XlsxBook {

	private OPCPackage pkg;
	private XSSFWorkbook book;

	public XlsxSourceBook(Owner owner, Path path) {
		super(owner, path);

		pkg = null;
		book = null;
	}

	public XSSFWorkbook getBook() {
		return book;
	}

	@Override
	public void open() throws IOException {

		try {
			pkg = OPCPackage.open(new java.io.File(getName()));
			book = new XSSFWorkbook(pkg);
		}
		catch (InvalidFormatException ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * Prepare to read from a workbook that has already been opened with open().
	 */
	@Override
	public void load() throws IOException {
		// No action is needed.
	}

	@Override
	public void close() throws IOException {

		pkg.close();
	}

	// Never called.
	@Override public void create() {}
	@Override public void append() {}
}
