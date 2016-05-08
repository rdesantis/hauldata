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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class DeleteTask extends Task {

	public DeleteTask(
			Prologue prologue,
			List<Expression<String>> files) {

		super(prologue);
		this.files = files;
	}

	@Override
	protected void execute(Context context) {

		Path path = null;
		DirectoryStream<Path> filePaths = null;
		try {
			for (Expression<String> file : files) {

				String fileName = file.evaluate();
				String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(fileName);
				Path parentPath = context.getWritePath(parentAndFileName[0]);
	
				try {
					filePaths = Files.newDirectoryStream(parentPath, parentAndFileName[1]);
				}
				catch (Exception ex) {
					String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
					throw new RuntimeException("Error occurred resolving file " + fileName + " - " + message, ex);
				}

				for (Path filePath : filePaths) {
					if (Files.isRegularFile(filePath)) {
						path = filePath;
						context.files.assureNotOpen(path);
						Files.delete(path);
					}
				}

				filePaths.close();
				filePaths = null;
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error deleting file " + path.toString() + " - " + message, ex);
		}
		finally { try {
			if (filePaths != null) filePaths.close();
		}
		catch (IOException ex) {
			throw new RuntimeException("Error occurred closing directory stream");
		} }
	}

	private List<Expression<String>> files;
}
