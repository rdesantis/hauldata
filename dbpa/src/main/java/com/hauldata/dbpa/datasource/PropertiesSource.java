/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.datasource;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class PropertiesSource implements Source {

	private Expression<String> fileName;
	private List<Expression<String>> propertyNames;

	private List<String> evaluatedPropertyNames;
	private Properties properties;
	private int rowIndex;

	public PropertiesSource(
			Expression<String> fileName,
			List<Expression<String>> propertyNames) {

		this.fileName = fileName;
		this.propertyNames = propertyNames;
	}

	@Override
	public void executeQuery(Context context) {

		// Evaluate argument expressions.

		String evaluatedFileName = fileName.evaluate();

		evaluatedPropertyNames = new ArrayList<String>();
		for (Expression<String> propertyName : propertyNames) {
			evaluatedPropertyNames.add(propertyName.evaluate());
		}

		// Read the properties file. 

		Path path = context.getPropertiesPath(evaluatedFileName);
		properties = new Properties();

		FileInputStream in = null;
		try {
			in = new FileInputStream(path.toString());
			properties.load(in);
		}
		catch (FileNotFoundException ex) {
			throw new RuntimeException("Properties file not found: " + evaluatedFileName);
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error reading properties from file \"" + evaluatedFileName + "\": " + message, ex);
		}
		finally {
			try { if (in != null) in.close(); }
			catch (Exception ex) {}
		}

		// Prepare to retrieve data.

		rowIndex = 0;
	}

	@Override
	public int getColumnCount() {
		if (evaluatedPropertyNames == null) {
			throw new RuntimeException("Query not executed");
		}
		return evaluatedPropertyNames.size();
	}

	@Override
	public String getColumnLabel(int column) {
		if (evaluatedPropertyNames == null) {
			throw new RuntimeException("Query not executed");
		}
		return evaluatedPropertyNames.get(column - 1);
	}

	@Override
	public boolean next() {
		return (++rowIndex == 1);
	}

	@Override
	public Object getObject(int columnIndex) {
		return properties.getProperty(getColumnLabel(columnIndex));
	}

	@Override
	public boolean isLast() {
		return (rowIndex == 1);
	}

	@Override
	public void done(Context context) {}

	@Override
	public void close(Context context) {
		evaluatedPropertyNames = null;
		properties = null;
	}
}
