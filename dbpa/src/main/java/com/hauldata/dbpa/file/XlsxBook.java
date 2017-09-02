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

import java.nio.file.Path;

/**
 * Microsoft Excel XLSX workbook
 */
public abstract class XlsxBook extends Book {

	private static final String typeName = "XLSX file";
	static public String typeName() { return typeName; }

	public static void registerHandler(String name) {

		Book.Factory targetBookFactory = new Book.Factory() {
			public Book instantiate(Node.Owner owner, Object path, FileOptions options) { return new XlsxTargetBook((File.Owner)owner, (Path)path); }
			public String getTypeName() { return typeName(); }
		};
		Sheet.Factory targetSheetFactory = new Sheet.Factory() {
			public Sheet instantiate(Node.Owner book, Object name, FileOptions options) { return new XlsxTargetSheet((Book)book, (String)name); }
			public String getTypeName() { return XlsxSheet.typeName(); }
		};

		Book.Factory sourceBookFactory = new Book.Factory() {
			public Book instantiate(Node.Owner owner, Object path, FileOptions options) { return new XlsxSourceBook((File.Owner)owner, (Path)path); }
			public String getTypeName() { return typeName(); }
		};
		Sheet.Factory sourceSheetFactory = new Sheet.Factory() {
			public Sheet instantiate(Node.Owner book, Object name, FileOptions options) { return new XlsxSourceSheet((Book)book, (String)name); }
			public String getTypeName() { return XlsxSheet.typeName(); }
		};

		FileHandler.register(name, true, new TargetSheetPage.Factory(targetBookFactory, targetSheetFactory), new SourceSheetPage.Factory(sourceBookFactory, sourceSheetFactory));
	}

	protected XlsxBook(Owner owner, Path path) {
		super(owner, path);
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}
}
