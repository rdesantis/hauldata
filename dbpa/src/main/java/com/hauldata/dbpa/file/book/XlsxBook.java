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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * Microsoft Excel XLSX workbook
 */
public abstract class XlsxBook extends Book {

	public interface WorkbookWrapper extends Closeable {
		Workbook getBook();
	}

	public interface WorkbookFactory {
		WorkbookWrapper newSourceBook(String filename) throws InvalidFormatException, IOException;
		WorkbookWrapper newTargetBook();
	}

	private String typeName;

	protected XlsxBook(String typeName, Owner owner, Path path) {
		super(owner, path);
		this.typeName = typeName;
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}
}
