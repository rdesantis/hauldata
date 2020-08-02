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

import java.nio.file.Path;

import com.hauldata.dbpa.file.File;
import com.hauldata.dbpa.file.FileHandler;
import com.hauldata.dbpa.file.Node;
import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.book.XlBook.WorkbookFactory;

public abstract class XlHandler {

	protected static void register(String name, String typeName, String sheetTypeName, WorkbookFactory factory) {

		Book.Factory targetBookFactory = new Book.Factory() {
			public Book instantiate(Node.Owner owner, Object path, PageOptions options) { return new XlTargetBook(typeName, sheetTypeName, factory, (File.Owner)owner, (Path)path); }
			public String getTypeName() { return typeName; }
		};
		Sheet.Factory targetSheetFactory = new Sheet.Factory() {
			public Sheet instantiate(Node.Owner book, Object name, PageOptions options) { return new XlTargetSheet(sheetTypeName, (Book)book, (String)name, (XlTargetSheet.TargetOptions)options); }
			public String getTypeName() { return sheetTypeName; }
		};

		Book.Factory sourceBookFactory = new Book.Factory() {
			public Book instantiate(Node.Owner owner, Object path, PageOptions options) { return new XlSourceBook(typeName, sheetTypeName, factory, (File.Owner)owner, (Path)path); }
			public String getTypeName() { return typeName; }
		};
		Sheet.Factory sourceSheetFactory = new Sheet.Factory() {
			public Sheet instantiate(Node.Owner book, Object name, PageOptions options) { return new XlSourceSheet(sheetTypeName, (Book)book, (String)name); }
			public String getTypeName() { return sheetTypeName; }
		};

		FileHandler.register(
				name, true,
				new TargetSheetPage.Factory(targetBookFactory, targetSheetFactory), new XlTargetSheet.TargetOptions.Parser(),
				new SourceSheetPage.Factory(sourceBookFactory, sourceSheetFactory), null);
	}
}
