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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import javax.naming.NamingException;

import com.hauldata.dbpa.process.DbProcess;

public class TestLoader implements Loader {

	private Map<String, String> scripts;

	/**
	 * @param scripts maps script names to script bodies
	 */
	public TestLoader(Map<String, String> scripts) {
		this.scripts = scripts;
	}

	@Override
	public DbProcess load(String name) throws IOException, NamingException {

		if (scripts == null) {
			throw new RuntimeException("Script map is not defined");
		}

		String script = scripts.get(name);
		if (script == null) {
			throw new FileNotFoundException("Script is not defined");
		}

		DbProcess process = DbProcess.parse(new StringReader(script));
		return process;
	}
}
