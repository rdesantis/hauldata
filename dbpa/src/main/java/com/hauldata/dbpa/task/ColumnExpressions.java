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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.expression.IntegerConstant;
import com.hauldata.dbpa.expression.StringConstant;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.Headers;

public class ColumnExpressions {

	private ArrayList<ExpressionBase> columns;
	
	public ColumnExpressions(ArrayList<ExpressionBase> columns) {
		this.columns = columns;
	}

	/**
	 * Validate column specification against the header specification
	 * to the extent possible at task parse time.
	 * @param headers
	 */
	public void validate(SourceHeaderExpressions headers) {

		if (!headers.exist()) {
			for (ExpressionBase column : columns) {
				if (column instanceof StringConstant) {
					String columnCaption = ((StringConstant)column).evaluate();
					throw new RuntimeException("Would attempt to read column at header \"" + columnCaption + "\" but file has no headers");
				}
			}
		}
		else if (headers.mustValidate()) {
			boolean allCaptionsAreConstant = headers.getCaptions().stream().allMatch(c -> c instanceof StringConstant);;

			for (ExpressionBase column : columns) {
				if (column instanceof IntegerConstant) {
					int columnIndex = ((IntegerConstant)column).evaluate();
					if (columnIndex > headers.getCaptions().size() || columnIndex <= 0) {
						throw new RuntimeException("Would attempt to read column beyond bounds of record at position " + String.valueOf(columnIndex));
					}
				}
				else if (column instanceof StringConstant) {
					if (allCaptionsAreConstant && !headers.getCaptions().contains((StringConstant)column)) {
						throw new RuntimeException("Would attempt to read column with non-existing header \"" + ((StringConstant)column).evaluate() + "\"");
					}
				}
			}
		}
	}

	/**
	 * Evaluate column expressions and resolve against headers at task run time.
	 * @param headers is the evaluated headers
	 * @return the evaluated columns resolved against the headers
	 */
	public Columns evaluate(Headers headers) {
		List<Object> positions = new LinkedList<Object>();
		
		for (ExpressionBase column : columns) {
			Object value = column.getEvaluationObject();
			if (value == null) {
				throw new RuntimeException("Column position expression evaluates to NULL");
			}
			positions.add(value);
		}

		return new Columns(positions, headers);
	}
}
