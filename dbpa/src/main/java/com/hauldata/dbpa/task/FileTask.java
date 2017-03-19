/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.SourcePage;
import com.hauldata.dbpa.file.TargetPage;
import com.hauldata.dbpa.process.Context;

public abstract class FileTask extends Task {

	public FileTask(Prologue prologue) {
		super(prologue);
	}

	protected void write(
			Context context,
			Source source,
			TargetPage page) {

		try {
			source.executeQuery(context);

			page.write(source);

			source.done(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("File write terminated due to interruption");
		}
		finally {
			source.close(context);
		}
	}

	protected void read(
			Context context,
			SourcePage page,
			SourceHeaders headers,
			Columns columns,
			DataTarget target) {

		try {
			target.prepareStatement(context, headers, columns);

			page.read(columns, target);
		}
		catch (SQLException ex) {
			DataTarget.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("File read terminated due to interruption");
		}
		finally {
			try { page.close(); } catch (Exception ex) {}

			target.close(context);
		}
	}
}
