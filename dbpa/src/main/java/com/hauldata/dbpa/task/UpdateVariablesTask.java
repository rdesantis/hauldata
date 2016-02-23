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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.variable.VariableBase;

public abstract class UpdateVariablesTask extends DatabaseTask {

	public UpdateVariablesTask(Prologue prologue) {
		super(prologue);
	}

	private boolean updateVariables(ResultSet rs, List<VariableBase> variables, boolean checkForOneRow) throws SQLException {

		boolean hasAnyRows = false;
		boolean hasOneRow = false;
		boolean hasRightNumberOfColumns = false;

		if (
				(hasAnyRows = rs.next()) &&
				(hasRightNumberOfColumns = (rs.getMetaData().getColumnCount() == variables.size()))) {

			if (checkForOneRow) {
				hasOneRow = rs.isLast();
			}

			int columnIndex = 1;
			for (VariableBase variable : variables) {
				variable.setValueChecked(rs.getObject(columnIndex++));
			}
		}

		if (!hasAnyRows && checkForOneRow) {
			throw new RuntimeException("Database query returned no rows");
		}
		else if (!hasOneRow && checkForOneRow) {
			throw new RuntimeException("Database query returned more than one row");
		}
		else if (hasAnyRows && !hasRightNumberOfColumns) {
			throw new RuntimeException("Database query returned different number of columns than variables to update");
		}
		
		return hasAnyRows;
	}

	/**
	 * Updates variables from the next row of the result set
	 * @param rs is the result set that is advanced to the next row.
	 * @param variables are the variables to be updated.
	 * @return true if a row remained and the variables were updated, false if no rows remained.
	 * @throws SQLException
	 * @throws RuntimeException if the number of columns did not match the number of variables to update.
	 */
	protected boolean updateVariables(ResultSet rs, List<VariableBase> variables) throws SQLException {
		return updateVariables(rs, variables, false);
	}

	/**
	 * Updates variables from the next row of the result set which must be the only remaining row
	 * @param rs is the result set that is advanced to the next row.
	 * @param variables are the variables to be updated.
	 * @throws SQLException
	 * @throws RuntimeException if there was not exactly one remaining row or the number of columns did not match the number of variables to update.
	 */
	protected void updateVariablesOnce(ResultSet rs, List<VariableBase> variables) throws SQLException {
		updateVariables(rs, variables, true);
	}
}
