/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.manage.resources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class FilesResource {

	private static final String cannotCreateMessageStem = "Cannot create %s file: ";
	private static final String notFoundMessageStem = "%s file not found: ";

	private String typeName;
	private String fileExt;
	private Path parentPath;

	public FilesResource(String typeName, String fileExt, Path parentPath) {
		this.typeName = typeName;
		this.fileExt = fileExt;
		this.parentPath = parentPath;
	}

	private Path getFilePath(String fileName) {
		return parentPath.resolve(fileName + fileExt).toAbsolutePath().normalize();
	}

	public String getCannotCreateMessageStem() {
		return String.format(cannotCreateMessageStem,  typeName);
	}

	public String getNotFoundMessageStem() {
		return String.format(notFoundMessageStem,  typeName);
	}

	/**
	 * Write a file.
	 * @param name is the name of the file
	 * @param body is the body of the file
	 * @throws IOException
	 */
	public void put(String name, String body) throws IOException {

		String filePathName = getFilePath(name).toString();

		Writer writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePathName)));

			writer.write(body);
		}
		catch (FileNotFoundException ex) {
			throw new FileNotFoundException(getCannotCreateMessageStem() + name + "; " + ex.getMessage());
		}
		finally {
			if (writer != null) {
				try { writer.close(); } catch (Exception ex) {}
			}
		}
	}

	/**
	 * Read a file.
	 * @param name is the name of the file
	 * @return the body of the file
	 * @throws FileNotFoundException if the file is not found
	 * @throws IOException for any other file system error
	 */
	public String get(String name) throws IOException {

		String filePathName = getFilePath(name).toString();

		final int bufferLength = 2048;
		char[] charBuffer = new char[bufferLength];
		StringBuilder bodyBuilder = new StringBuilder();

		Reader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePathName)));

			int count;
			while((count = reader.read(charBuffer, 0, bufferLength)) != -1) {
				bodyBuilder.append(charBuffer, 0, count);
			}
		}
		catch (FileNotFoundException ex) {
			throw new FileNotFoundException(getNotFoundMessageStem() + name);
		}
		finally {
			if (reader != null) {
				try { reader.close(); } catch (Exception ex) {}
			}
		}

		return bodyBuilder.toString();
	}

	/**
	 * Delete a file.
	 * @param name is the name of the file
	 * @throws NoSuchFileException if the file is not found
	 * @throws IOException for any other file system error
	 */
	public void delete(String name) throws IOException  {
		try {
			Files.delete(getFilePath(name));
		}
		catch (NoSuchFileException ex) {
			throw new NoSuchFileException(getNotFoundMessageStem() + name);
		}
	}

	/**
	 * Retrieve a list of names of all files available in the respective directory
	 * that match an optional name wildcard pattern
	 *
	 * @param likeName is the wildcard pattern or null to match all names.  It may include
	 * a leading relative path to find files in nested directories.  If so, names are returned
	 * with the path prefixed.  It is not an error if the path does not exist.
	 *
	 * @return the list of files if any exists;
	 * or an empty list if no files exist
	 * @throws IOException
	 */
	public  List<String> getNames(String likeName) throws IOException {

		if (likeName == null) {
			likeName = "*";
		}

		final int fileExtLength = fileExt.length();

		String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(likeName);
		Path fullParentPath = parentPath.resolve(parentAndFileName[0]);
		String coreLikeName = parentAndFileName[1];
		String resultNamePrefix = parentAndFileName[0].equals(".") ? "" : parentAndFileName[0] + "/";

		List<String> fileNames = new LinkedList<String>();

		DirectoryStream<java.nio.file.Path> filePaths = null;
		try {
			filePaths = Files.newDirectoryStream(fullParentPath, coreLikeName + fileExt);

			for (java.nio.file.Path filePath : filePaths) {
				if (Files.isRegularFile(filePath)) {

					String fileName = filePath.getFileName().toString();
					String name = resultNamePrefix + fileName.substring(0, fileName.length() - fileExtLength);

					fileNames.add(name);
				}
			}
		}
		catch (NotDirectoryException | NoSuchFileException ex) {
			// Not an error; just return empty result
		}
		finally {
			try { filePaths.close(); } catch (Exception ex) {}
		}

		return fileNames;
	}
}
