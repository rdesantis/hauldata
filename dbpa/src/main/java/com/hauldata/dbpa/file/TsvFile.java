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
import java.nio.file.Path;

/**
 * Tab Separated Values file.
 */
public class TsvFile extends DsvFile {

	private static final String typeName = "TSV file";
	static public String typeName() { return typeName; }

	public static void registerHandler(String name) {
		File.Factory fileFactory = new File.Factory() {
			public File instantiate(Node.Owner owner, Object path, FileOptions options) { return new TsvFile((File.Owner)owner, (Path)path, options); }
			public String getTypeName() { return typeName; }
		};
		FileHandler.register(name, false, new TargetFilePage.Factory(fileFactory), new TargetOptions.Factory(), null, null);
	}
	
	public TsvFile(Owner owner, Path path, FileOptions options) {
		super(owner, path, '\t', options);
	}

	// Node overrides

	@Override
	public String getTypeName() {
		return typeName;
	}

	// PageNode overrides

	@Override
	public void writeColumn(int columnIndex, Object object) throws IOException {
		if (1 < columnIndex) {
			writer.write(separator);
		}

		if (object != null) {
			writer.write(object.toString());
		}

		if (columnIndex == headers.getColumnCount()) {
			writer.write(String.format("%n"));
		}
	}

	@Override
	public Object readColumn(int columnIndex) throws IOException {
		throw new RuntimeException("Read from TSV not implemented");
	}
}
