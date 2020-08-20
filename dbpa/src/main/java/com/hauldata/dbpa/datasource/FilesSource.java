/*
 * Copyright (c) 2019, Ronald DeSantis
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

package com.hauldata.dbpa.datasource;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Iterator;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class FilesSource implements Source {

	private Expression<String> fileNamePattern;
	private boolean writeNotRead;

	private DirectoryStream<Path> paths;
	private Iterator<Path> pathIterator;
	private String fileName;
	private String nextFileName;

	public FilesSource(
			Expression<String> fileNamePattern,
			boolean writeNotRead) {
		this.fileNamePattern = fileNamePattern;
		this.writeNotRead = writeNotRead;
	}

	@Override
	public boolean hasMetadata() {
		return true;
	}

	@Override
	public void executeQuery(Context context) throws SQLException, InterruptedException {

		String evaluatedPattern = fileNamePattern.evaluate();

		String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(evaluatedPattern);
		Path parentPath = context.getDataPath(parentAndFileName[0], writeNotRead);

		paths = null;
		try {
			paths = Files.newDirectoryStream(parentPath, parentAndFileName[1]);
			pathIterator = paths.iterator();
			nextFileName = getNextFileName();
		}
		catch (IOException ex) {
			throw new RuntimeException(ex.toString());
		}
	}

	private String getNextFileName() {

		while (pathIterator.hasNext()) {
			Path path = pathIterator.next();
			if (Files.isRegularFile(path)) {
				return path.getFileName().toString();
			}
		}
		return null;
	}

	@Override
	public int getColumnCount() {
		return 1;
	}

	@Override
	public String getColumnLabel(int column) {
		return "name";
	}

	@Override
	public boolean next() {

		boolean hasNext = !isLast();
		fileName = nextFileName;
		nextFileName = getNextFileName();
		return hasNext;
	}

	@Override
	public Object getObject(int columnIndex) {
		return fileName;
	}

	@Override
	public boolean isLast() {
		return (nextFileName == null);
	}

	@Override
	public void done(Context context) {}

	@Override
	public void close(Context context) {
		if (paths != null) {
			try { paths.close(); } catch (Exception ex) {}
		}
	}
}
