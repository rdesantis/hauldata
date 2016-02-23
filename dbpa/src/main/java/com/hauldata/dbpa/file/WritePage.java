/*
 * Copyright (c) 2016, Ronald DeSantis
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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class WritePage {

	interface Factory {
		/**
		 * Physically create the file or sheet with the indicated headers and
		 * return the page for writing.  Throw an exception if the file or
		 * sheet can't be created either physically or logically. 
		 */
		WritePage create(File.Owner fileOwner, PageIdentifier id, WriteHeaders headers) throws IOException;

		/**
		 * Physically position the file or sheet for appending and
		 * return the page for writing.  Throw an exception if the file or
		 * sheet can't be positioned either physically or logically. 
		 */
		WritePage append(File.Owner fileOwner, PageIdentifier id) throws IOException;

		/**
		 * Physically create the file or sheet with the indicated headers or position
		 * it for appending, depending on whether it has already been created, and 
		 * return the page for writing.  Throw an exception if the file or
		 * sheet can't be created or appended either physically or logically. 
		 */
		WritePage write(File.Owner fileOwner, PageIdentifier id, WriteHeaders headers) throws IOException;
	}

	private PageNode node;

	protected WritePage(PageNode node) {
		this.node = node;
	}

	public WriteHeaders getWriteHeaders() {
		return node.getWriteHeaders();
	}

	/**
	 * @see PageNode#writeColumn(int, Object)
	 */
	public void writeColumn(int columnIndex, Object object) throws IOException {
		node.writeColumn(columnIndex, object);
	}

	/**
	 * Write result set to the page
	 */
	public void write(ResultSet rs) throws SQLException {

		boolean hasAnyRows = false;
		boolean hasRightNumberOfColumns = false;

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int resultColumnCount = metaData.getColumnCount();
			
			WriteHeaders headers = node.getWriteHeaders();
			if (headers.getColumnCount() == 0) {
				headers.setColumnCount(resultColumnCount);

				if (headers.fromMetadata()) {

					for (int columnIndex = 1; columnIndex <= resultColumnCount; ++columnIndex) {
						node.writeColumn(columnIndex, metaData.getColumnLabel(columnIndex));
					}
				}
				
				headers.setWritten();
			}

			if (
					(hasAnyRows = rs.next()) &&
					(hasRightNumberOfColumns = (resultColumnCount == headers.getColumnCount()))) {

				do {
					for (int columnIndex = 1; columnIndex <= headers.getColumnCount(); ++columnIndex) {
						node.writeColumn(columnIndex, rs.getObject(columnIndex));
					}
				} while (rs.next());
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
