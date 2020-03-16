/*
 * Copyright (c) 2020, Ronald DeSantis
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

package com.hauldata.dbpa;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.process.DbProcess;

/**
 * CheckDbp - Check Database Process Script Syntax.
 * Command line utility to construct DbProcess for multiple files
 */
public class CheckDbp {

	public static final String programName = "CheckDbp";

	public static void main(String[] args) {

		if (args.length != 1) {
			System.err.println("File name pattern was not provided");
			System.exit(1);
		}

		String searchPattern = args[0];
		String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(searchPattern);
		String searchPathName = parentAndFileName[0];
		String fileNamePattern = parentAndFileName[1] + '.' + FileLoader.processFileExt;

		ContextProperties contextProps = new ContextProperties(DBPA.runProgramName);
		String processPathName = contextProps.getProperties("path").getProperty("process");
		Path processPath = Paths.get(processPathName);

		Path searchPath = processPath.resolve(searchPathName).toAbsolutePath().normalize();

		int allCount = 0;
		int badCount = 0;
		DirectoryStream<Path> paths = null;
		try {
			paths = Files.newDirectoryStream(searchPath, fileNamePattern);

			for (Path path : paths) {
				Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString())));
				try {
					++allCount;
					DbProcess.parse(reader);
				}
				catch (RuntimeException ex) {
					++badCount;
					System.err.println(path.toString() + ": " + ex.getMessage());
				}
			}
		}
		catch (Exception ex) {
			System.err.println(ex.toString());
			System.exit(2);
		}

		System.out.println(String.valueOf(allCount) + " files read, " + String.valueOf(badCount) + " files failed");
	}
}
