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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class UnzipTask extends Task {

	public UnzipTask(
			Prologue prologue,
			Expression<String> source,
			Expression<String> target) {

		super(prologue);
		this.source = source;
		this.target = target;
	}

	@Override
	protected void execute(Context context) {

		// See http://www.oracle.com/technetwork/articles/java/compress-1565076.html

		final int BUFFER = 2048;

		ZipInputStream in = null;
		BufferedOutputStream out = null;

		try {
			String targetDirectory = context.getDataPath(target.evaluate()).toString();
			
			in = new ZipInputStream(
					new BufferedInputStream(
							new FileInputStream(context.getDataPath(source.evaluate()).toString())));

			byte data[] = new byte[BUFFER];

			ZipEntry entry;
			while ((entry = in.getNextEntry()) != null) {

				Path targetPath = com.hauldata.dbpa.process.Files.getPath(targetDirectory, entry.getName());
				out = new BufferedOutputStream(
						new FileOutputStream(targetPath.toString()), BUFFER);
	
				int count;
				while ((count = in.read(data, 0, BUFFER)) != -1) {
					out.write(data, 0, count);
				}

		        out.flush();
		        out.close();
		        out = null;
		        
		        Files.setLastModifiedTime(targetPath, entry.getLastModifiedTime());
		     }
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred unzipping file: " + message, ex);
		}
		finally { try {
			if (out != null) out.close();
			if (in != null) in.close();
		}
		catch (IOException ex) {
			throw new RuntimeException("Error occurred closing ZIP file");
		} }
	}

	Expression<String> source;
	Expression<String> target;
}
