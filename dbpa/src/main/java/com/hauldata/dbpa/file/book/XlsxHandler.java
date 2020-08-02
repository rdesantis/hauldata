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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.hauldata.dbpa.file.book.XlBook.WorkbookFactory;
import com.hauldata.dbpa.file.book.XlBook.WorkbookWrapper;

public class XlsxHandler extends XlHandler {

	private static class XlsxSourceBookWrapper implements WorkbookWrapper {
		private OPCPackage pkg = null;
		private XSSFWorkbook book = null;

		public XlsxSourceBookWrapper(String filename) throws FileNotFoundException, IOException, InvalidFormatException {
			pkg = OPCPackage.open(new java.io.File(filename));
			book = new XSSFWorkbook(pkg);
		}

		@Override
		public Workbook getBook() {
			return book;
		}

		@Override
		public void close() throws IOException {
			if (pkg != null) {
				pkg.close();
			}
		}
	}

	private static class XlsxTargetBookWrapper implements WorkbookWrapper {
		SXSSFWorkbook book = null;

		public XlsxTargetBookWrapper() {
			book = new SXSSFWorkbook();
		}

		@Override
		public Workbook getBook() {
			return book;
		}

		@Override
		public void close() throws IOException {
			if (book != null) {
				book.dispose();
			}
		}
	}

	public static void register(String name) {
		register(name, "XLSX File", "XLSX Sheet", new WorkbookFactory() {

			@Override
			public WorkbookWrapper newSourceBook(String filename) throws InvalidFormatException, IOException {
				return new XlsxSourceBookWrapper(filename);
			}

			@Override
			public WorkbookWrapper newTargetBook() {
				return new XlsxTargetBookWrapper();
			}
		});
	}
}
