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

public interface PageIdentifier {

	public abstract String getName();

	/**
	 * Creates a file or sheet with the indicated headers and returns the page for writing
	 * @param fileOwner is a collection that tracks open files
	 * @param headers are the headers to write
	 * @return the TargetPage object that allows writing to the file or sheet
	 * @throws RuntimeException if this operation is inconsistent with previous usage of the file or sheet
	 * @throws IOException
	 */
	public TargetPage create(File.Owner fileOwner, PageOptions options, TargetHeaders headers) throws IOException;

	/**
	 * Opens the indicated file or sheet for appending; if already open, this is a NOP
	 * @param fileOwner is a collection that tracks open files
	 * @param headers are the headers to write
	 * @return the TargetPage object that allows writing to the file or sheet
	 * @throws RuntimeException if this operation is inconsistent with previous usage of the file or sheet
	 * @throws IOException
	 */
	public TargetPage append(File.Owner fileOwner) throws IOException;

	/**
	 * If the indicated file or sheet is not open, creates it with the indicated headers; if already open, this is a NOP
	 * @param fileOwner is a collection that tracks open files
	 * @param headers are the headers to write
	 * @return the TargetPage object that allows writing to the file or sheet
	 * @throws RuntimeException if this operation is inconsistent with previous usage of the file or sheet
	 * @throws IOException
	 */
	public TargetPage write(File.Owner fileOwner, PageOptions options, TargetHeaders headers) throws IOException;

	/**
	 * Opens the indicated file or sheet with the indicated headers and returns the page for reading
	 * @param fileOwner is a collection that tracks open files
	 * @param headers are the headers to validate
	 * @return the SourcePage object that allows reading from the file or sheet
	 * @throws RuntimeException if this operation is inconsistent with previous usage of the file or sheet
	 * @throws IOException
	 */
	public SourcePage open(File.Owner fileOwner, PageOptions options, SourceHeaders headers) throws IOException;

	/**
	 * Positions the open file or sheet for loading and return the page for reading
	 * @param fileOwner is a collection that tracks open files
	 * @return the SourcePage object that allows reading from the file or sheet
	 * @throws RuntimeException if this operation is inconsistent with previous usage of the file or sheet
	 * @throws IOException
	 */
	public SourcePage load(File.Owner fileOwner) throws IOException;

	/**
	 * Opens the file or sheet with the indicated headers or positions it for loading,
	 * depending on whether it has already been opened, and returns the page for reading
	 * @param fileOwner is a collection that tracks open files
	 * @return the SourcePage object that allows reading from the file or sheet
	 * @throws RuntimeException if this operation is inconsistent with previous usage of the file or sheet
	 * @throws IOException
	 */
	public SourcePage read(File.Owner fileOwner, PageOptions options, SourceHeaders headers) throws IOException;
}
