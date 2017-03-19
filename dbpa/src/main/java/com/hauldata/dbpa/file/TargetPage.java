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

import com.hauldata.dbpa.datasource.Source;

public abstract class TargetPage {

	interface Factory {
		/**
		 * Physically create the file or sheet with the indicated headers and
		 * return the page for writing.  Throw an exception if the file or
		 * sheet can't be created either physically or logically. 
		 */
		TargetPage create(File.Owner fileOwner, PageIdentifier id, TargetHeaders headers) throws IOException;

		/**
		 * Physically position the file or sheet for appending and
		 * return the page for writing.  Throw an exception if the file or
		 * sheet can't be positioned either physically or logically. 
		 */
		TargetPage append(File.Owner fileOwner, PageIdentifier id) throws IOException;

		/**
		 * Physically create the file or sheet with the indicated headers or position
		 * it for appending, depending on whether it has already been created, and 
		 * return the page for writing.  Throw an exception if the file or
		 * sheet can't be created or appended either physically or logically. 
		 */
		TargetPage write(File.Owner fileOwner, PageIdentifier id, TargetHeaders headers) throws IOException;
	}

	private PageNode node;

	protected TargetPage(PageNode node) {
		this.node = node;
	}

	public TargetHeaders getWriteHeaders() {
		return node.getTargetHeaders();
	}

	/**
	 * @see PageNode#writeColumn(int, Object)
	 */
	public void writeColumn(int columnIndex, Object object) throws IOException {
		node.writeColumn(columnIndex, object);
	}

	/**
	 * Write result set to the page
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void write(Source source) throws SQLException, InterruptedException {

		boolean hasAnyRows = false;
		boolean hasRightNumberOfColumns = false;

		try {
			int resultColumnCount = source.getColumnCount();
			
			TargetHeaders headers = node.getTargetHeaders();
			if (headers.getColumnCount() == 0) {
				headers.setColumnCount(resultColumnCount);

				if (headers.fromMetadata()) {

					for (int columnIndex = 1; columnIndex <= resultColumnCount; ++columnIndex) {
						node.writeColumn(columnIndex, source.getColumnLabel(columnIndex));
					}
				}
				
				headers.setWritten();
			}

			if (
					(hasAnyRows = source.next()) &&
					(hasRightNumberOfColumns = (resultColumnCount == headers.getColumnCount()))) {

				do {
					for (int columnIndex = 1; columnIndex <= headers.getColumnCount(); ++columnIndex) {
						node.writeColumn(columnIndex, source.getObject(columnIndex));
					}
				} while (source.next());
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred writing file: " + node.getName() + ": " + message);
		}

		if (hasAnyRows && !hasRightNumberOfColumns) {
			throw new RuntimeException("Database returned the wrong number of columns for this file: " + node.getName());
		}
	}
}
