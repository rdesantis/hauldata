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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import javax.naming.NamingException;

import com.hauldata.dbpa.process.DbProcess;

public class FileLoader implements Loader {

	public static final String processFileExt = "dbp";

	public FileLoader(String processPath) {
		this.processPath = processPath;
	}

	@Override
	public DbProcess load(String name) throws IOException, NamingException {

		String processFileName = name + "." + processFileExt; 
		File processFile = new File(processPath, processFileName);
		Reader reader = new BufferedReader(new FileReader(processFile));
		return DbProcess.parse(reader);
	}

	private String processPath;
}
