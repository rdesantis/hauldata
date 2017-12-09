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

import java.nio.file.Path;

import com.hauldata.dbpa.file.FileHandler;
import com.hauldata.dbpa.file.PhysicalPageIdentifier;

public class SheetIdentifier extends PhysicalPageIdentifier {

	private String sheetName;

	public SheetIdentifier(FileHandler handler, Path path, String sheetName) {
		super(handler, path);
		this.sheetName = sheetName;
	}

	String getSheetName() {
		return sheetName;
	}

	@Override
	public String getName() {
		return path.toString() + "!" + sheetName;
	}
}
