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

import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.variable.VariableBase;

public abstract class UpdateVariablesTask extends DatabaseTask {

	public UpdateVariablesTask(Prologue prologue) {
		super(prologue);
	}

	private boolean updateVariables(DataSource source, List<VariableBase> variables, boolean checkForSingleRow) throws SQLException, InterruptedException {

		boolean hasAnyRows = false;
		boolean hasSingleRow = false;
		boolean hasRightNumberOfColumns = false;

		if (
				(hasAnyRows = source.next()) &&
				(hasRightNumberOfColumns = (source.getColumnCount() == variables.size()))) {

			if (checkForSingleRow) {
				hasSingleRow = source.isLast();
			}

			int columnIndex = 1;
			for (VariableBase variable : variables) {
				variable.setValueChecked(DataSource.fromSQL(source.getObject(columnIndex++)));
			}
		}

		if (!hasAnyRows && checkForSingleRow) {
			throw new RuntimeException("Database query returned no rows");
		}
		else if (!hasSingleRow && checkForSingleRow) {
			throw new RuntimeException("Database query returned more than one row");
		}
		else if (hasAnyRows && !hasRightNumberOfColumns) {
			throw new RuntimeException("Database query returned different number of columns than variables to update");
		}

		return hasAnyRows;
	}

	/**
	 * Updates variables from the next row of the result set
	 * @param source is the data source that is advanced to the next row.
	 * @param variables are the variables to be updated.
	 * @return true if a row remained and the variables were updated, false if no rows remained.
	 * @throws SQLException
	 * @throws InterruptedException
	 * @throws RuntimeException if the number of columns did not match the number of variables to update.
	 */
	protected boolean updateVariables(DataSource source, List<VariableBase> variables) throws SQLException, InterruptedException {
		return updateVariables(source, variables, false);
	}

	/**
	 * Updates variables from the next row of the result set which must be the only remaining row
	 * @param source is the data source that is advanced to the next row.
	 * @param variables are the variables to be updated.
	 * @throws SQLException
	 * @throws InterruptedException
	 * @throws RuntimeException if there was not exactly one remaining row or the number of columns did not match the number of variables to update.
	 */
	protected void updateVariablesOnce(DataSource source, List<VariableBase> variables) throws SQLException, InterruptedException {
		updateVariables(source, variables, true);
	}
}
