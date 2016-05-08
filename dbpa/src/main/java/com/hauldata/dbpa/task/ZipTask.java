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

package com.hauldata.dbpa.task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class ZipTask extends Task {

	public ZipTask(
			Prologue prologue,
			ArrayList<Expression<String>> sources,
			Expression<String> target) {

		super(prologue);
		this.sources = sources;
		this.target = target;
	}

	@Override
	protected void execute(Context context) {

		// See http://www.oracle.com/technetwork/articles/java/compress-1565076.html

		final int BUFFER = 2048;

		ZipOutputStream out = null;
		DirectoryStream<Path> paths = null;
		BufferedInputStream in = null;

		try {
			String targetName = target.evaluate();
			out = new ZipOutputStream(
					new BufferedOutputStream(
							new FileOutputStream(context.getWritePath(targetName).toFile())));

			byte data[] = new byte[BUFFER];

			for (Expression<String> source : sources) {
				
				String sourceName = source.evaluate();
				String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(sourceName);
				Path parentPath = context.getWritePath(parentAndFileName[0]);

				try {
					paths = Files.newDirectoryStream(parentPath, parentAndFileName[1]);
				}
				catch (Exception ex) {
					String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
					throw new RuntimeException("Error occurred resolving FROM file " + sourceName + " - " + message, ex);
				}
				
				for (Path path : paths) {

					BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
					if (attrs.isRegularFile()) {

						context.files.assureNotOpen(path);

						ZipEntry entry = new ZipEntry(path.getFileName().toString());
						entry.setCreationTime(attrs.creationTime());
						entry.setLastModifiedTime(attrs.lastModifiedTime());
						entry.setLastAccessTime(attrs.lastAccessTime());
		
						out.putNextEntry(entry);
		
						in = new BufferedInputStream(
								new FileInputStream(path.toFile()), BUFFER);
		
						int count;
						while((count = in.read(data, 0, BUFFER)) != -1) {
							out.write(data, 0, count);
						}
						
						in.close();
						in = null;
					}
				}
				
				paths.close();
				paths = null;
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred zipping file: " + message, ex);
		}
		finally { try {
			if (in != null) in.close();
			if (paths != null) paths.close();
			if (out != null) out.close();
		}
		catch (IOException ex) {
			throw new RuntimeException("Error occurred closing ZIP file");
		} }
	}

	ArrayList<Expression<String>> sources;
	Expression<String> target;
}
