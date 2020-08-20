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

import java.util.Iterator;
import java.util.List;

import org.apache.commons.vfs2.FileSystemException;

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class FtpSource implements Source {

	private FtpConnection connection;
	private Expression<String> fileNamePattern;
	private Iterator<String> nameIterator;
	private String fileName;

	public FtpSource(FtpConnection connection, Expression<String> fileNamePattern) {
		this.connection = connection;
		this.fileNamePattern = fileNamePattern;
		nameIterator = null;
	}

	@Override
	public boolean hasMetadata() {
		return true;
	}

	@Override
	public void executeQuery(Context context) {

		String evaluatedPattern = fileNamePattern.evaluate();

		String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(evaluatedPattern);

		try {
			List<String> foundNames = context.getManager(connection, false).findRemote(parentAndFileName[0], parentAndFileName[1]);
			nameIterator = foundNames.iterator();
		}
		catch (FileSystemException ex) {
			throw new RuntimeException(ex.toString());
		}
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
		if (hasNext) {
			fileName = nameIterator.next();
		}
		return hasNext;
	}

	@Override
	public Object getObject(int columnIndex) {
		return fileName;
	}

	@Override
	public boolean isLast() {
		return !nameIterator.hasNext();
	}

	@Override
	public void done(Context context) {}

	@Override
	public void close(Context context) {
		nameIterator = null;
	}
}
