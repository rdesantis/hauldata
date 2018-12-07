/*
 * Copyright (c) 2018, Ronald DeSantis
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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.PhysicalPageIdentifier;
import com.hauldata.dbpa.file.fixed.DataFixedFields;
import com.hauldata.dbpa.file.fixed.FirstTrailerFixedFields;
import com.hauldata.dbpa.file.fixed.FixedFields;
import com.hauldata.dbpa.file.fixed.KeeperFixedField;
import com.hauldata.dbpa.file.flat.TxtFile;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.task.expression.PageIdentifierExpression;
import com.hauldata.dbpa.task.expression.fixed.DataFixedFieldExpressions;
import com.hauldata.dbpa.task.expression.fixed.FixedFieldExpressions;

public class ReadFixedTask extends Task {

	private PageIdentifierExpression page;
	private List<FixedFieldExpressions> headers;
	private DataFixedFieldExpressions dataFields;
	private List<FixedFieldExpressions> trailers;
	private DataTarget target;

	public ReadFixedTask(
			Prologue prologue,
			PageIdentifierExpression page,
			List<FixedFieldExpressions> headers,
			DataFixedFieldExpressions dataFields,
			List<FixedFieldExpressions> trailers,
			DataTarget target) {

		super(prologue);
		this.page = page;
		this.headers = headers;
		this.dataFields = dataFields;
		this.trailers = trailers;
		this.target = target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		PageIdentifier page = this.page.evaluate(context, false);
		List<FixedFields> headers = evaluate(this.headers, true);
		DataFixedFields dataFields = this.dataFields.evaluate();
		List<FixedFields> trailers = evaluate(this.trailers, false);
		FirstTrailerFixedFields firstTrailer = trailers.isEmpty() ? null : (FirstTrailerFixedFields)trailers.get(0);

		Path sourcePath = ((PhysicalPageIdentifier)page).getPath();
		context.files.assureNotOpen(sourcePath);
		TxtFile sourcePage = new TxtFile(context.files, sourcePath, null);
		try {
			sourcePage.open();

			read(sourcePage, headers, true);

			read(sourcePage, dataFields, firstTrailer, target, context);

			read(sourcePage, trailers, false);

			if (sourcePage.hasRow()) {
				throw new RuntimeException("End of file not found as expected after the trailer record(s)");
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading " + page.getName() + ": " + message);
		}
		finally {
			if (sourcePage.isOpen()) { try { sourcePage.close(); } catch (Exception ex) {} }
		}
	}

	private List<FixedFields> evaluate(List<FixedFieldExpressions> fieldsPerRecord, boolean headerNotTrailer) {

		boolean firstTrailer = !headerNotTrailer;

		List<FixedFields> result = new LinkedList<FixedFields>();
		for (FixedFieldExpressions fields : fieldsPerRecord) {
			if (firstTrailer) {
				result.add(fields.evaluateFirstTrailer());
				firstTrailer = false;
			}
			else {
				result.add(fields.evaluate());
			}
		}
		return result;
	}

	private void read(
			TxtFile sourcePage,
			List<FixedFields> fieldsPerRecord,
			boolean headerNotTrailer) throws IOException {

		for (FixedFields fields : fieldsPerRecord) {

			if (!sourcePage.hasRow()) {
				throw new RuntimeException("End of file encountered when expecting a " + (headerNotTrailer ? "header" : "trailer") + " record");
			}

			String record = (String)sourcePage.readColumn(1);
			fields.actOn(record);
			sourcePage.readColumn(2);
		}
	}

	private void read(
			TxtFile sourcePage,
			DataFixedFields dataFields,
			FirstTrailerFixedFields firstTrailer,
			DataTarget target,
			Context context) throws IOException {

		try {
			target.prepareStatement(context, null, null);

			while (sourcePage.hasRow()) {

				String record = (String)sourcePage.readColumn(1);
				if ((firstTrailer != null) && firstTrailer.matches(record)) {
					break;
				}
				dataFields.actOn(record);
				sourcePage.readColumn(2);

				int targetColumnIndex = 1;
				for (KeeperFixedField field : dataFields.getKeeperFields()) {
					target.setObject(targetColumnIndex++, field.getValue());
				}
				target.addBatch();
			}

			if (!sourcePage.hasRow() && (firstTrailer != null)) {
				throw new RuntimeException("End of file encountered when expecting a trailer record");
			}

			target.executeBatch();
		}
		catch (SQLException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Database statement execution failed: " + message, ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("File read terminated due to interruption");
		}
		finally {
			target.close(context);
		}
	}
}
