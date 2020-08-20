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

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.datasource.Target;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.process.Context;

public class FlowTask extends Task {

	private Source source;
	private Target target;

	public FlowTask(Prologue prologue, Source source, Target target) {
		super(prologue);
		this.source = source;
		this.target = target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		try {
			source.executeQuery(context);
			target.prepareStatement(context, getColumns());

			if ((0 <= target.getParameterCount()) && (target.getParameterCount() != source.getColumnCount())) {
				throw new RuntimeException("Number of source columns does not match the number of target parameters");
			}

			while (source.next()) {
				for (int i = 1; i <= source.getColumnCount(); ++i) {
					target.setObject(i, source.getObject(i));
				}
				target.addBatch();
			}

			target.executeBatch();
			source.done(context);
		}
		finally {
			target.close(context);
			source.close(context);
		}
	}

	private Columns getColumns() throws SQLException {
		if (source.hasMetadata()) {
			List<String> columnNames = new LinkedList<String>();
			for (int i = 1; i <= source.getColumnCount(); ++i) {
				columnNames.add(source.getColumnLabel(i));
			}
			return new Columns(columnNames);
		}
		else {
			return new Columns(source.getColumnCount());
		}
	}
}
