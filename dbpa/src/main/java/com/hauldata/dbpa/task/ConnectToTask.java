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

import java.io.IOException;
import java.io.StringReader;
import java.util.NoSuchElementException;
import java.util.Properties;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.tokenizer.KeywordValueTokenizer;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Token;

public class ConnectToTask extends ConnectTask {

	private boolean inherit;
	private Expression<String> properties;

	public ConnectToTask(
			Prologue prologue,
			Connection connection,
			boolean inherit,
			Expression<String> properties) {

		super(prologue, connection);
		this.inherit = inherit;
		this.properties = properties;
	}

	@Override
	protected Properties getConnectionProperties(Context context) {

		String keywordValues = this.properties.evaluate();
		if (keywordValues == null) {
			throw new RuntimeException("Connection properties expression evaluates to NULL");
		}
		return parse(keywordValues, inherit, defaultProperties(context, connection));
	}

	private Properties parse(
			String keywordValues,
			boolean inherit,
			Properties defaults) {

		Properties properties = inherit ? new Properties(defaults) : new Properties();

		KeywordValueTokenizer tokenizer = new KeywordValueTokenizer(new StringReader(keywordValues));
		try {
			while (tokenizer.hasNext()) {
				String keyword = tokenizer.nextWord();

				Token valueToken = tokenizer.nextToken();
				if (valueToken == null) {
					throw new RuntimeException("Missing value in connection properties");
				}
				String value = valueToken instanceof Quoted ? ((Quoted)valueToken).getBody() : valueToken.getImage();

				properties.setProperty(keyword, value);
			}
		}
		catch (NoSuchElementException ex) { // also catches InputMismatchException
			throw new RuntimeException("Property name not found where expected in connection properties");
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to read connection properties: " + message, ex);
		}
		finally {
			try { tokenizer.close(); } catch (Exception ex) {}
		}

		return properties;
	}
}
