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

package com.hauldata.dbpa.expression;

import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.datasource.DataStore;
import com.hauldata.dbpa.datasource.ValuesSource;
import com.hauldata.dbpa.variable.Table;
import com.hauldata.dbpa.variable.VariableType;

public class TableFromValues extends Expression<Table> {
	private ValuesSource source;

	public TableFromValues(ValuesSource source) {
		super(VariableType.TABLE);
		this.source = source;
	}

	@Override
	public Table evaluate() {

		Table result = new Table();

		source.executeQuery(null);
		while (source.next()) {
			List<Object> valuesList = new LinkedList<Object>();
			for (int i = 1; i <= source.getColumnCount(); ++i) {
				valuesList.add(DataStore.fromSQL(source.getObject(i)));
			}
			result.add(valuesList);
		}
		source.done(null);
		source.close(null);

		return result;
	}
}
