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

package com.hauldata.dbpa.task;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.datasource.TableDataTarget;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.ReadHeaders;
import com.hauldata.dbpa.process.Context;

public abstract class RequestTask extends Task {

	protected Expression<String> url;
	protected Expression<String> header;

	protected List<Expression<String>> requestFields;
	protected DataSource source;
	protected List<Expression<String>> keepFields;
	protected List<Expression<String>> responseFields;
	protected Expression<String> statusField;
	protected DataTarget target;

	public static class Parameters {

		public Expression<String> url;
		public Expression<String> header;

		public List<Expression<String>> requestFields;
		public DataSource source;
		public List<Expression<String>> keepFields;
		public List<Expression<String>> responseFields;
		public Expression<String> statusField;
		public DataTarget target;
	}

	protected RequestTask(Prologue prologue, Parameters parameters) {
		super(prologue);

		url = parameters.url;
		header = parameters.header;

		requestFields = parameters.requestFields;
		source = parameters.source;
		keepFields = parameters.keepFields;
		responseFields = parameters.responseFields;
		statusField = parameters.statusField;
		target = parameters.target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		// Evaluate arguments.

		String url = this.url.evaluate();
		String header = this.header.evaluate();

		List<String> requestFields = new ArrayList<String>();
		for (Expression<String> requestField : this.requestFields) {
			requestFields.add(requestField.evaluate());
		}

		List<String> keepFields = null;
		if (this.keepFields != null) {

			keepFields = new ArrayList<String>();
			for (Expression<String> keepFieldExpression : this.keepFields) {

				String keepField = keepFieldExpression.evaluate();
				if (keepField == null) {
					throw new RuntimeException("KEEP field evaluates to NULL");
				}
				if (!requestFields.contains(keepField)) {
					throw new RuntimeException("KEEP field does not evaluate to a request field value");
				}
				keepFields.add(keepField);
			}
		}

		List<String> responseFields = new ArrayList<String>();
		if (this.responseFields != null) {

			for (Expression<String> responseFieldExpression : this.responseFields) {

				String responseField = responseFieldExpression.evaluate();
				if (responseField == null) {
					throw new RuntimeException("RESPONSE field evaluates to NULL");
				}
				responseFields.add(responseField);
			}
		}

		String statusField = this.statusField.evaluate();
		if (statusField == null) {
			throw new RuntimeException("STATUS field evaluates to NULL");
		}

		// Determine the following:
		// 1. Which requestFields will substitute into the URL
		// 2. Which requestFields will be retained in the result row
		// 3. How many columns will be in the result row

		Set<Integer> substituteIndexes = new TreeSet<Integer>();
		for (int i = 0; i < requestFields.size(); ++i) {
			if (url.contains("{" + requestFields.get(i) + "}")) {
				substituteIndexes.add(i + 1);
			}
		}

		List<Integer> keepIndexes = new LinkedList<Integer>();
		if (keepFields == null) {
			// Keep all non-NULL named request fields.

			for (int i = 0; i < requestFields.size(); ++i) {
				String requestField = requestFields.get(i);
				if (requestField != null) {
					keepIndexes.add(i + 1);
				}
			}
		}
		else {
			// Respect KEEP clause.

			for (String keepField : keepFields) {
				keepIndexes.add(requestFields.indexOf(keepField) + 1);
			}
		}

		int fieldCount = keepIndexes.size() + responseFields.size();

		try {
			// Prepare to write target rows.  Include preparation needed to write to table.

			ReadHeaders headers = null;
			Columns columns = null;

			if (target instanceof TableDataTarget) {

				ArrayList<String> captions = new ArrayList<String>();
				for (int keepIndex : keepIndexes) {
					captions.add(requestFields.get(keepIndex - 1));
				}
				for (String responseField : responseFields) {
					captions.add(responseField);
				}

				headers = new ReadHeaders(true, false, false, captions);
				columns = new Columns(null, headers);
			}

			target.prepareStatement(context, headers, columns);

			// Loop over source rows.

			source.executeQuery(context);

			while (source.next()) {
				String resolvedUrl = url;

				if (!substituteIndexes.isEmpty()) {
					for (int substituteIndex : substituteIndexes) {

						String template = "{" + requestFields.get(substituteIndex - 1) + "}";
						String replacement = (String)source.getObject(substituteIndex);
						resolvedUrl = resolvedUrl.replace(template, replacement);
					}
				}

				// Construct request.

				Client client = ClientBuilder.newClient();

				WebTarget webTarget = client.target(resolvedUrl);

				// TODO!!!
			}

			source.done(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Web service request was terminated due to interruption");
		}
		finally {
			source.close(context);
		}
	}
}
