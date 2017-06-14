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
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class FilesResource {

	private String fileExt;
	private Path parentPath;

	public FilesResource(String fileExt, Path parentPath) {
		this.fileExt = fileExt;
		this.parentPath = parentPath;
	}

	private Path getFilePath(String fileName) {
		return parentPath.resolve(fileName + fileExt).toAbsolutePath().normalize();
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
		Files.delete(getFilePath(name));
	}

	/**
	 * Retrieve a list of names of all files available in the respective directory
	 * that match an optional name wildcard pattern
	 *
	 * @param likeName is the wildcard pattern or null to match all names
	 * @return the list of files if any exists;
	 * or an empty list if no files exist
	 * @throws IOException
	 */
	public  List<String> getNames(String likeName) throws IOException {

		if (likeName == null) {
			likeName = "*";
		}

		final int fileExtLength = fileExt.length();

		List<String> fileNames = new LinkedList<String>();

		DirectoryStream<java.nio.file.Path> filePaths = null;
		try {
			filePaths = Files.newDirectoryStream(parentPath, likeName + fileExt);

			for (java.nio.file.Path filePath : filePaths) {
				if (Files.isRegularFile(filePath)) {

					String fileName = filePath.getFileName().toString();
					String name = fileName.substring(0, fileName.length() - fileExtLength);

					fileNames.add(name);
				}
			}
		}
		finally {
			try { filePaths.close(); } catch (Exception ex) {}
		}

		return fileNames;
	}
}
