/*
 * Copyright (c) 2017, 2019, Ronald DeSantis
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

import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.SourceOptions;
import com.hauldata.dbpa.process.Context;

public class TableDataTarget extends DataTarget {

	Expression<String> table;
	Expression<String> prefix;

	public TableDataTarget(
			DatabaseConnection connection,
			Expression<String> table,
			Expression<String> prefix) {

		super(connection);
		this.table = table;
		this.prefix = prefix;
	}

	@Override
	public void prepareStatement(Context context, SourceOptions options, SourceHeaders headers, Columns columns) throws SQLException {

		String table = this.table.evaluate();
		String prefix = (this.prefix != null) ? this.prefix.evaluate() : null;

		List<String> captions = columns.getCaptions();

		StringBuilder statement = new StringBuilder();
		if (prefix != null) {
			statement.append(prefix).append(" ");
		}

		statement.append("INSERT INTO ").append(table).append(" ");

		if (headers.toMetadata()) {
			statement.append("(");

			String separator = "";
			for (String caption : captions) {
				if (caption.length() == 0) {
					throw new RuntimeException("File has a blank column header - not allowed when headers are not explicitly provided");
				}
				statement.append(separator).append(caption);
				separator = ", ";
			}
			statement.append(") ");
		}

		statement.append("VALUES (");

		String separator = "";
		for (int i = 0; i < captions.size(); ++i) {
			statement.append(separator).append("?");
			separator = ",";
		}

		statement.append(")");

		prepareStatement(context, options, statement.toString());
	}
}
