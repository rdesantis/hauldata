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
import java.sql.SQLException;

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.util.tokenizer.EndOfLine;

public abstract class SourcePage {

	interface Factory {
		/**
		 * Physically open the file or sheet with the indicated headers and
		 * return the page for reading.  Throw an exception if the file or
		 * sheet can't be opened either physically or logically.
		 */
		SourcePage open(File.Owner fileOwner, PageIdentifier id, SourceHeaders headers) throws IOException;

		/**
		 * Physically position the open file or sheet for loading and
		 * return the page for reading.  Throw an exception if the file or
		 * sheet can't be positioned either physically or logically.
		 */
		SourcePage load(File.Owner fileOwner, PageIdentifier id) throws IOException;

		/**
		 * Physically open the file or sheet with the indicated headers or position
		 * it for loading, depending on whether it has already been opened, and
		 * return the page for reading.  Throw an exception if the file or
		 * sheet can't be opened or loaded either physically or logically.
		 */
		SourcePage read(File.Owner fileOwner, PageIdentifier id, SourceHeaders headers) throws IOException;
	}

	private PageNode node;

	protected SourcePage(PageNode node) {
		this.node = node;
	}

	public SourceHeaders getReadHeaders() {
		return node.getSourceHeaders();
	}

	/**
	 * @see PageNode#readColumn(int)
	 */
	public Object readColumn(int columnIndex) throws IOException {
		return node.readColumn(columnIndex);
	}

	/**
	 * @throws InterruptedException
	 * @see PageNode#hasRow()
	 */
	public boolean hasRow() throws IOException, InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		return node.hasRow();
	}

	public void close() throws IOException {
		node.close();
	}

	/**
	 * Read selected columns from page to target
	 * @throws InterruptedException
	 */
	public void read(Columns columns, DataTarget target) throws SQLException, InterruptedException {

		// See http://stackoverflow.com/questions/12012592/jdbc-insert-multiple-rows
		// and http://www.java2s.com/Code/JavaAPI/java.sql/PreparedStatementaddBatch.htm

		boolean hasWrongNumberOfColumns = false;

		int parameterCount = 0;
		try {
			parameterCount = target.getParameterCount();
		}
		catch (SQLException ex) {} // Not all databases support getParameterMetaData() in all cases

		try {
			if ((columns.size() == 0) && (parameterCount > 0)) {
				columns.setSize(parameterCount);
			}

			if (!(hasWrongNumberOfColumns = (parameterCount > 0) && (columns.size() > 0) && (parameterCount != columns.size()))) {

				while (hasRow()) {
					int sourceColumnIndex = 1;
					for (Object value = null; (value = node.readColumn(sourceColumnIndex)) != EndOfLine.value; ++sourceColumnIndex) {
						int[] targetColumnIndexes = columns.getTargetColumnIndexes(sourceColumnIndex);
						for (int targetColumnIndex : targetColumnIndexes) {
							target.setObject(targetColumnIndex, value);
						}
					}
					target.addBatch();
				}
				target.executeBatch();
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading file: " + node.getName() + ": " + message);
		}

		if (hasWrongNumberOfColumns) {
			throw new RuntimeException("The file has wrong number of columns for the requested operation: " + node.getName());
		}
	}
}
