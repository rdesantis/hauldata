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

package com.hauldata.dbpa.task;

import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.Table;
import com.hauldata.dbpa.variable.Variable;

public class InsertTask extends Task {
	private Variable<Table> variable;
	private Source source;

	public InsertTask(
			Prologue prologue,
			Variable<Table> variable,
			Source source) {
		super(prologue);
		this.variable = variable;
		this.source = source;
	}

	@Override
	protected void execute(Context context) throws Exception {
		source.executeQuery(context);
		while (source.next()) {
			List<Object> values = new LinkedList<Object>();
			for (int columnIndex = 1; columnIndex <= source.getColumnCount(); ++columnIndex) {
				values.add(source.getObject(columnIndex));
			}
			variable.getValue().add(values);
		}
		source.done(context);
		source.close(context);
	}
}
