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

public interface PageNode {

	public void setHeaders(Headers headers);

	public SourceHeaders getSourceHeaders();

	public TargetHeaders getTargetHeaders();

	/**
	 * @return the fully-qualified human-readable name of the node
	 */
	public String getName();

	/**
	 * Write a value to the next column of the current row.
	 * The first column has index 1.  This must be called with ascending
	 * columnIndex values starting at 1 and not exceeding the number of
	 * columns specified in the WriteHeaders.
	 * @param columnIndex is the index of the column to write, starting at 1
	 * @param object is the object whose value is to be written
	 * @throws IOException 
	 */
	public void writeColumn(int columnIndex, Object object) throws IOException;

	public void flush() throws IOException;

	/**
	 * Read a value from the next column of the current row or
	 * EndOfLine.value if no columns remain.  An empty column
	 * returns null; an empty string in a column returns a zero-length
	 * String object. 
	 * 
	 * The first column has index 1.  This must be called with ascending
	 * columnIndex values starting at 1 and stopping at exactly the
	 * established number of columns plus 1.  The established number of
	 * columns is the number of columns specified in the ReadHeaders, if any;
	 * or, the number passed subsequently to setColumnCount(); or, if neither
	 * of those apply, the established number of columns is determined
	 * from the first row read.
	 * @param columnIndex is the index of the column to read, starting at 1
	 * @return the value read from the column or EndOfLine.value if no columns remain
	 * @throws IOException 
	 */
	public Object readColumn(int columnIndex) throws IOException;

	/**
	 * @return true if the page has at least one more row to be read
	 * @throws IOException
	 */
	public boolean hasRow() throws IOException;

	public abstract void close() throws IOException;
}
