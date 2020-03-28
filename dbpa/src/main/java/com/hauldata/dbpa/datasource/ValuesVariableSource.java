/*
 * Copyright (c) 2020, Ronald DeSantis
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

import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.Values;
import com.hauldata.dbpa.variable.Variable;

public class ValuesVariableSource implements Source {

	private Variable<Values> variable;

	private ListIterator<List<Object>> rowIterator;
	private List<Object> valuesList;

	public ValuesVariableSource(Variable<Values> variable) {
		this.variable = variable;
	}

	@Override
	public void executeQuery(Context context) {
		rowIterator = variable.getValue().getValuesLists().listIterator();
	}

	@Override
	public int getColumnCount() {
		List<List<Object>> valuesLists = variable.getValue().getValuesLists();
		return valuesLists.isEmpty() ? 0 : valuesLists.get(0).size();
	}

	@Override
	public String getColumnLabel(int column) {
		return null;
	}

	@Override
	public boolean next() {

		if (isLast()) return false;

		valuesList = rowIterator.next();

		return true;
	}

	@Override
	public Object getObject(int columnIndex) {
		return valuesList.get(columnIndex - 1);
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
