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
import java.nio.file.StandardCopyOption;
import java.util.List;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class CopyTask extends RenameOrCopyTask {

	public CopyTask(
			Prologue prologue,
			List<Expression<String>> from,
			Expression<String> to) {

		super(prologue);
		this.from = from;
		this.to = to;
	}

	@Override
	protected void execute(Context context) {

		Path toPath = context.getWritePath(to.evaluate());
		boolean toDirectory = Files.isDirectory(toPath);

		if ((from.size() > 1) && !toDirectory) {
			throw new RuntimeException("Cannot copy multiple source files to the same target file; target must be an existing directory");
		}

		DirectoryStream<Path> sourcePaths = null;
		try {
			for (Expression<String> source : from) {
				
				String sourceName = source.evaluate();
				String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(sourceName);
				Path parentPath = context.getWritePath(parentAndFileName[0]);
	
				try {
					sourcePaths = Files.newDirectoryStream(parentPath, parentAndFileName[1]);
				}
				catch (Exception ex) {
					String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
					throw new RuntimeException("Error occurred resolving FROM file " + sourceName + " - " + message, ex);
				}
				
				for (Path sourcePath : sourcePaths) {
					if (Files.isRegularFile(sourcePath)) {
						Path targetPath = toDirectory ? toPath.resolve(sourcePath.getFileName()) : toPath;
						doAction(context, sourcePath, targetPath);
					}
				}

				sourcePaths.close();
				sourcePaths = null;
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred copying file: " + message, ex);
		}
		finally { try {
			if (sourcePaths != null) sourcePaths.close();
		}
		catch (IOException ex) {
			throw new RuntimeException("Error occurred closing directory stream");
		} }
	}

	@Override
	protected String verb() {
		return "copy";
	}

	@Override
	protected void action(Path from, Path to) throws IOException {
		Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
	}

	private List<Expression<String>> from;
	private Expression<String> to;
}
