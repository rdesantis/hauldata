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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.PhysicalPageIdentifier;
import com.hauldata.dbpa.file.fixed.DataFixedFieldsTarget;
import com.hauldata.dbpa.file.fixed.FixedFields;
import com.hauldata.dbpa.file.fixed.KeeperFixedField;
import com.hauldata.dbpa.file.flat.TxtFile;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.task.expression.PageIdentifierExpression;
import com.hauldata.dbpa.task.expression.fixed.DataFixedFieldExpressionsTarget;
import com.hauldata.dbpa.task.expression.fixed.FixedFieldExpressions;

public class ReadFixedTask extends Task {

	private PageIdentifierExpression page;
	private List<FixedFieldExpressions> headers;
	private List<DataFixedFieldExpressionsTarget> dataRecordTargets;
	private List<FixedFieldExpressions> trailers;

	public ReadFixedTask(
			Prologue prologue,
			PageIdentifierExpression page,
			List<FixedFieldExpressions> headers,
			List<DataFixedFieldExpressionsTarget> dataRecordTargets,
			List<FixedFieldExpressions> trailers) {

		super(prologue);
		this.page = page;
		this.headers = headers;
		this.dataRecordTargets = dataRecordTargets;
		this.trailers = trailers;
	}

	@Override
	protected void execute(Context context) throws Exception {

		PageIdentifier page = this.page.evaluate(context, false);
		List<FixedFields> headers = evaluate(this.headers, true);
		List<DataFixedFieldsTarget> dataRecordTargets = evaluate(this.dataRecordTargets);
		List<FixedFields> trailers = evaluate(this.trailers, false);
		FixedFields firstTrailer = !trailers.isEmpty() ? trailers.get(0) : null;

		Path sourcePath = ((PhysicalPageIdentifier)page).getPath();
		context.files.assureNotOpen(sourcePath);
		TxtFile sourcePage = new TxtFile(context.files, sourcePath, null);
		try {
			sourcePage.open();

			read(sourcePage, headers, true);

			read(sourcePage, dataRecordTargets, firstTrailer, context);

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

	private List<FixedFields> evaluate(List<FixedFieldExpressions> fieldsPerRecord, boolean isHeaderNotTrailer) {

		List<FixedFields> result = new LinkedList<FixedFields>();
		for (FixedFieldExpressions fields : fieldsPerRecord) {
			result.add(fields.evaluate());
		}
		return result;
	}

	private List<DataFixedFieldsTarget> evaluate(List<DataFixedFieldExpressionsTarget> fieldsPerRecord) {

		List<DataFixedFieldsTarget> result = new ArrayList<DataFixedFieldsTarget>();
		for (DataFixedFieldExpressionsTarget fields : fieldsPerRecord) {
			result.add(fields.evaluate());
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
			List<DataFixedFieldsTarget> dataRecordTargets,
			FixedFields firstTrailer,
			Context context)  throws IOException {

		try {
			for (DataFixedFieldsTarget dataRecordTarget : dataRecordTargets) {
				dataRecordTarget.getTarget().prepareStatement(context, null, null);
			}

			final int maxLevel = dataRecordTargets.size() - 1;
			int level = -1;
			while (sourcePage.hasRow()) {

				String record = (String)sourcePage.readColumn(1);
				if ((firstTrailer != null) && firstTrailer.matches(record)) {
					break;
				}
				sourcePage.readColumn(2);

				if (!dataRecordTargets.isEmpty()) {

					while ((level < maxLevel) && !dataRecordTargets.get(++level).hasJoin());
					while ((0 <= level) && !dataRecordTargets.get(level).matches(record)) {
						--level;
					}
					if (level < 0) {
						throw new RuntimeException("Record does not match expected data record or first trailer");
					}

					DataFixedFieldsTarget dataRecordTarget = dataRecordTargets.get(level);
					dataRecordTarget.actNonMatchersOn(record);

					DataTarget target = dataRecordTarget.getTarget();
					int targetColumnIndex = 1;
					for (int lowerLevel = 0; lowerLevel < level; ++lowerLevel) {
						for (KeeperFixedField field : dataRecordTargets.get(lowerLevel).getJoinedFields()) {
							target.setObject(targetColumnIndex++, field.getValue());
						}
					}
					for (KeeperFixedField field : dataRecordTarget.getKeeperFields()) {
						target.setObject(targetColumnIndex++, field.getValue());
					}
					target.addBatch();
				}
			}

			if (!sourcePage.hasRow() && (firstTrailer != null)) {
				throw new RuntimeException("End of file encountered when expecting a trailer record");
			}

			for (DataFixedFieldsTarget dataRecordTarget : dataRecordTargets) {
				dataRecordTarget.getTarget().executeBatch();
			}
		}
		catch (SQLException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Database statement execution failed: " + message, ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("File read terminated due to interruption");
		}
		finally {
			for (DataFixedFieldsTarget dataRecordTarget : dataRecordTargets) {
				dataRecordTarget.getTarget().close(context);
			}
		}
	}
}
