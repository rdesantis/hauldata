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

package com.hauldata.dbpa.loader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.naming.NamingException;

import com.hauldata.dbpa.process.DbProcess;

public class FileLoader implements Loader {

	public static final String processFileExt = "dbp";

	private String processPathString;

	public FileLoader(String processPathString) {
		this.processPathString = processPathString;
	}

	public String getProcessPathString() {
		return processPathString;
	}

	/**
	 * @throws FileNotFoundException if the file is not found
	 * @throws IOException for any other file system error
	 */
	@Override
	public DbProcess load(String name) throws IOException, NamingException {

		String processFileName = name + "." + processFileExt;
		Path path = Paths.get(processPathString).resolve(processFileName);
		Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString())));
		return DbProcess.parse(reader);
	}
}
