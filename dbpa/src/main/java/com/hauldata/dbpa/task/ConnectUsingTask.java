/*
 * Copyright (c) 2019, Ronald DeSantis
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Properties;

import com.hauldata.dbpa.DBPA;
import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.Files;

public class ConnectUsingTask extends ConnectTask {

	private boolean inherit;
	private Expression<String> fileNameTrunk;

	public ConnectUsingTask(
			Prologue prologue,
			Connection connection,
			boolean inherit,
			Expression<String> fileNameTrunk) {

		super(prologue, connection);
		this.inherit = inherit;
		this.fileNameTrunk = fileNameTrunk;
	}

	@Override
	protected Properties getConnectionProperties(Context context) {

		String fileNameTrunk = this.fileNameTrunk.evaluate();
		if (fileNameTrunk == null) {
			throw new RuntimeException("Connection file name expression evaluates to NULL");
		}
		String fileName = "Connect." + fileNameTrunk + ".properties";
		Path path = Files.getPath(DBPA.home, fileName);

		Properties properties = inherit ? new Properties(defaultProperties(context, connection)) : new Properties();

		FileInputStream in = null;
		try {
			in = new FileInputStream(path.toString());
			properties.load(in);
		}
		catch (FileNotFoundException ex) {
			throw new RuntimeException("Could not find file: " + fileName);
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error reading properties from file \"" + fileName + "\": " + message, ex);
		}
		finally {
			try { if (in != null) in.close(); }
			catch (Exception ex) {}
		}

		return properties;
	}
}
