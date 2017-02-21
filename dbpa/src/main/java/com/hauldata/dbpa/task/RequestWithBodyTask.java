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
import java.util.List;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.expression.Expression;

public abstract class RequestWithBodyTask extends RequestTask {

	public static class ParametersWithBody extends Parameters {

		private List<Expression<String>> bodyFields;

		public ParametersWithBody() {}

		public void add(List<Expression<String>> bodyFields) {
			this.bodyFields = bodyFields;
		}

		public List<Expression<String>> getBodyFields() { return bodyFields; }
	}

	public RequestWithBodyTask(
			Prologue prologue,
			DataSource source,
			Parameters parameters,
			DataTarget target) {
		super(prologue, source, parameters, target);
	}

	protected static class EvaluatedParametersWithBody extends EvaluatedParameters {

		private List<String> bodyFields;

		public EvaluatedParametersWithBody() {}

		public void add(List<String> bodyFields) {
			this.bodyFields = bodyFields;
		}

		public List<String> getBodyFields() { return bodyFields; }
	}

	@Override
	public EvaluatedParameters makeEvaluatedParameters() { return new EvaluatedParametersWithBody(); }

	@Override
	protected void evaluateBody(Parameters baseRawParameters, EvaluatedParameters baseEvaluatedParameters) {

		ParametersWithBody rawParameters = (ParametersWithBody)baseRawParameters;
		EvaluatedParametersWithBody parameters = (EvaluatedParametersWithBody)baseEvaluatedParameters;

		List<String> bodyFields = new ArrayList<String>();
		for (Expression<String> requestField : rawParameters.bodyFields) {
			bodyFields.add(requestField.evaluate());
		}

		if (!allFieldsAreUnique(bodyFields)) {
			throw new RuntimeException("Body fields do not evaluate to unique values");
		}

		parameters.add(bodyFields);
	}

	@Override
	protected int getBodyFieldsIndexOf(EvaluatedParameters baseEvaluatedParameters, String keepField) {
		return ((EvaluatedParametersWithBody)baseEvaluatedParameters).getBodyFields().indexOf(keepField);
	}

	@Override
	protected int getSourceColumnCount(EvaluatedParameters baseEvaluatedParameters) {

		EvaluatedParametersWithBody parameters = (EvaluatedParametersWithBody)baseEvaluatedParameters;

		return super.getSourceColumnCount(parameters) + parameters.getBodyFields().size();
	}

	@Override
	protected List<String> getBodyFields(EvaluatedParameters baseEvaluatedParameters) {
		return ((EvaluatedParametersWithBody)baseEvaluatedParameters).getBodyFields();
	}

	@Override
	protected void addBody(HttpRequestBase baseRequest, EvaluatedParameters baseEvaluatedParameters, DataSource source) throws SQLException {

		HttpEntityEnclosingRequestBase request = (HttpEntityEnclosingRequestBase)baseRequest;
		EvaluatedParametersWithBody parameters = (EvaluatedParametersWithBody)baseEvaluatedParameters;

		JsonObjectBuilder builder = Json.createObjectBuilder();

		int columnIndex = super.getSourceColumnCount(parameters);
		for (String fieldName : parameters.getBodyFields()) {
			++columnIndex;
			if (fieldName != null) {
				Object fieldValue = source.getObject(columnIndex);
				if (fieldValue != null) {
					builder.add(fieldName, fieldValue.toString());
				}
				else {
					builder.addNull(fieldName);
				}
			}
		}

		String body = builder.build().toString();

		StringEntity entity = new StringEntity(body, supportedContentType);

		request.setEntity(entity);
	}
}
