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

import com.hauldata.dbpa.file.PageOptions;

/**
 * Microsoft Excel XLSX worksheet
 */
public abstract class XlsxSheet extends com.hauldata.dbpa.file.book.Sheet {

	static final String typeName = "XLSX sheet";
	static public String typeName() { return typeName; }

	public XlsxSheet(Book owner, String name, PageOptions options) {
		super(owner, name, options);
	}

	public XlsxSheet(Book owner, String name) {
		this(owner, name, null);
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}
}
