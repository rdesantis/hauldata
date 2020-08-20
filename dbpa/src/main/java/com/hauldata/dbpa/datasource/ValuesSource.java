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

import java.util.List;
import java.util.ListIterator;

import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;

/**
 * Source provided from a VALUES clause.
 * <p>
 * The VALUES clause must have at least one row.  All rows must have the same number of columns.
 * Individual values in a row may be constants or expressions including variables.
 * Expressions in a row are not evaluated until the cursor is moved to the row by next().
 */
public class ValuesSource implements Source {

	private List<ExpressionBase[]> values;
	private Object[] rowEvaluatedValues;

	private ListIterator<ExpressionBase[]> rowIterator;

	public ValuesSource(List<ExpressionBase[]> values) {

		this.values = values;
		rowEvaluatedValues = new Object[getColumnCount()];
	}

	@Override
	public boolean hasMetadata() {
		return false;
	}

	@Override
	public void executeQuery(Context context) {
		rowIterator = values.listIterator();
	}

	@Override
	public int getColumnCount() {
		return values.get(0).length;
	}

	@Override
	public String getColumnLabel(int column) {
		return null;
	}

	@Override
	public boolean next() {

		if (isLast()) return false;

		ExpressionBase[] rowValues = rowIterator.next();
		for (int i = 0; i < rowValues.length; ++i) {
			rowEvaluatedValues[i] = DataStore.toSQL(rowValues[i].getEvaluationObject(), rowValues[i].getType());
		}

		return true;
	}

	@Override
	public Object getObject(int columnIndex) {
		return rowEvaluatedValues[columnIndex - 1];
	}

	@Override
	public boolean isLast() {
		return !rowIterator.hasNext();
	}

	@Override
	public void done(Context context) {}

	@Override
	public void close(Context context) {}
}
