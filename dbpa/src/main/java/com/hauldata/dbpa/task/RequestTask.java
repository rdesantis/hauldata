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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.datasource.TableDataTarget;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.ReadHeaders;
import com.hauldata.dbpa.process.Context;

public abstract class RequestTask extends Task {

	public static final ContentType supportedContentType = ContentType.APPLICATION_JSON;
	public static final int maxResponseContentLength = 2048;

	public static class Parameters {

		public static class Header {
			public Expression<String> name;
			public boolean isNullable;
			public Expression<String> value;

			public Header(
					Expression<String> name,
					boolean isNullable,
					Expression<String> value) {

				this.name = name;
				this.isNullable = isNullable;
				this.value = value;
			}
		}

		private Expression<String> url;
		private List<Header> headers;

		private List<Expression<String>> requestFields;
		private List<Expression<String>> keepFields;
		private List<Expression<String>> responseFields;
		private Expression<String> statusField;
		private Expression<String> messageField;

		public Parameters() {}

		public void add(
				Expression<String> url,
				List<Header> headers) {

			this.url = url;
			this.headers = headers;
		}

		public void add(
				List<Expression<String>> requestFields,
				List<Expression<String>> keepFields,
				List<Expression<String>> responseFields,
				Expression<String> statusField,
				Expression<String> messageField) {

			this.requestFields = requestFields;
			this.keepFields = keepFields;
			this.responseFields = responseFields;
			this.statusField = statusField;
			this.messageField = messageField;
		}

		public Expression<String> getUrl() { return url; }
		public List<Header> getHeaders() { return headers; }
		public List<Expression<String>> getRequestFields() { return requestFields; }
		public List<Expression<String>> getKeepFields() { return keepFields; }
		public List<Expression<String>> getResponseFields() { return responseFields; }
		public Expression<String> getStatusField() { return statusField; }
		public Expression<String> getMessageField() { return messageField; }
	}

	protected DataSource source;
	protected Parameters parameters;
	protected DataTarget target;

	protected RequestTask(
			Prologue prologue,
			DataSource source,
			Parameters parameters,
			DataTarget target) {
		super(prologue);

		this.source = source;
		this.parameters = parameters;
		this.target = target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		EvaluatedParameters evaluatedParameters = evaluateParameters(parameters);

		executeRequest(context, source, evaluatedParameters, target);
	}

	protected static class EvaluatedParameters {

		public static class Header {
			public String name;
			public String value;

			public Header(
					String name,
					String value) {

				this.name = name;
				this.value = value;
			}
		}

		private String url;
		private List<Header> headers;

		private List<String> requestFields;
		private Set<Integer> substituteIndexes;
		private List<Integer> keepIndexes;
		private List<String> responseFields;
		private String statusField;
		private String messageField;


		public EvaluatedParameters() {}

		public void add(
				String url,
				List<Header> headers,

				List<String> requestFields,
				Set<Integer> substituteIndexes,
				List<Integer> keepIndexes,
				List<String> responseFields,
				String statusField,
				String messageField) {

			this.url = url;
			this.headers = headers;

			this.requestFields = requestFields;
			this.substituteIndexes = substituteIndexes;
			this.keepIndexes = keepIndexes;
			this.responseFields = responseFields;
			this.statusField = statusField;
			this.messageField = messageField;
		}

		public String getUrl() { return url; }
		public List<Header> getHeaders() { return headers; }

		public List<String> getRequestFields() { return requestFields; }
		public Set<Integer> getSubstituteIndexes() { return substituteIndexes; }
		public List<Integer> getKeepIndexes() { return keepIndexes; }
		public List<String> getResponseFields() { return responseFields; }
		public String getStatusField() { return statusField; }
		public String getMessageField() { return messageField; }
	}

	private EvaluatedParameters evaluateParameters(Parameters rawParameters) throws Exception {

		EvaluatedParameters parameters = makeEvaluatedParameters();

		evaluateBody(rawParameters, parameters);

		// Evaluate base parameters.

		String url = rawParameters.getUrl().evaluate();
		if (url == null) {
			throw new RuntimeException("URL evaluates to NULL");
		}

		List<EvaluatedParameters.Header> headers = new LinkedList<EvaluatedParameters.Header>();
		if (rawParameters.getHeaders() != null) {

			for (Parameters.Header header : rawParameters.getHeaders()) {

				String name = header.name.evaluate();
				String value = header.value.evaluate();
				if (name != null) {
					headers.add(new EvaluatedParameters.Header(name, value));
				}
				else if (!header.isNullable) {
					throw new RuntimeException("Non-nullable header name evaluates to NULL");
				}
			}
		}

		List<String> requestFields = new ArrayList<String>();
		for (Expression<String> requestField : rawParameters.getRequestFields()) {
			requestFields.add(requestField.evaluate());
		}

		if (!allFieldsAreUnique(requestFields)) {
			throw new RuntimeException("Request fields do not evaluate to unique values");
		}

		Set<Integer> substituteIndexes = new TreeSet<Integer>();
		for (int i = 0; i < requestFields.size(); ++i) {

			String requestField = requestFields.get(i);
			if ((requestField != null) && url.contains("{" + requestField + "}")) {
				substituteIndexes.add(i + 1);
			}
		}

		List<Integer> keepIndexes = new LinkedList<Integer>();
		if (rawParameters.getKeepFields() != null) {
			// Respect KEEP clause.

			for (Expression<String> keepFieldExpression : rawParameters.getKeepFields()) {

				String keepField = keepFieldExpression.evaluate();
				if (keepField == null) {
					throw new RuntimeException("KEEP field evaluates to NULL");
				}

				int requestFieldIndex = requestFields.indexOf(keepField);
				int bodyFieldIndex = getBodyFieldsIndexOf(parameters, keepField);
				int fieldIndex;

				if (0 <= requestFieldIndex && 0 <= bodyFieldIndex) {
					throw new RuntimeException("KEEP field is ambiguous; name appears as both a request field and body field value");
				}
				else if (0 <= requestFieldIndex) {
					fieldIndex = requestFieldIndex;
				}
				else if (0 <= bodyFieldIndex) {
					fieldIndex = requestFields.size() + bodyFieldIndex;
				}
				else {
					throw new RuntimeException("KEEP field does not evaluate to a request or body field value");
				}

				keepIndexes.add(fieldIndex + 1);
			}
		}
		else {
			// Keep all non-NULL named request fields.

			for (int i = 0; i < requestFields.size(); ++i) {
				String requestField = requestFields.get(i);
				if (requestField != null) {
					keepIndexes.add(i + 1);
				}
			}
		}

		List<String> responseFields = new ArrayList<String>();
		if (rawParameters.getResponseFields() != null) {

			for (Expression<String> responseFieldExpression : rawParameters.getResponseFields()) {

				String responseField = responseFieldExpression.evaluate();
				if (responseField == null) {
					throw new RuntimeException("RESPONSE field evaluates to NULL");
				}
				responseFields.add(responseField);
			}
		}

		String statusField = rawParameters.getStatusField().evaluate();
		if (statusField == null) {
			throw new RuntimeException("STATUS field evaluates to NULL");
		}

		String messageField = null;
		if (rawParameters.getMessageField() != null) {
			messageField = rawParameters.getMessageField().evaluate();
		}

		// Bundle up the parameters.

		parameters.add(url, headers, requestFields, substituteIndexes, keepIndexes, responseFields, statusField, messageField);

		return parameters;
	}

	protected EvaluatedParameters makeEvaluatedParameters() { return new EvaluatedParameters(); }

	protected void evaluateBody(Parameters baseRawParameters, EvaluatedParameters baseEvaluatedParameters) {}

	protected int getBodyFieldsIndexOf(EvaluatedParameters baseEvaluatedParameters, String keepField) { return -1; }

	protected boolean allFieldsAreUnique(List<String> fields) {
		return fields.stream().collect(Collectors.toSet()).size() == fields.size();
	}

	private void executeRequest(Context context, DataSource source, EvaluatedParameters parameters, DataTarget target) {

		int sourceColumnCount = getSourceColumnCount(parameters);
		int targetParameterCount = getTargetParameterCount(parameters);

		CloseableHttpClient client = null;
		HttpRequestBase request = null;
		CloseableHttpResponse response = null;

		try {
			// Prepare to write target rows.

			ReadHeaders headers = null;
			Columns columns = null;

			if (target instanceof TableDataTarget) {
				// Prepare to write to table.

				ArrayList<String> captions = new ArrayList<String>();

				for (int keepIndex : parameters.getKeepIndexes()) {

					List<String> fields;
					int fieldIndex;
					if (keepIndex <= parameters.getRequestFields().size()) {
						fields = parameters.getRequestFields();
						fieldIndex = keepIndex;
					}
					else {
						fields = getBodyFields(parameters);
						fieldIndex = keepIndex - parameters.getRequestFields().size();
					}
					captions.add(fields.get(fieldIndex - 1));
				}

				for (String responseField : parameters.getResponseFields()) {
					captions.add(responseField);
				}

				captions.add(parameters.getStatusField());

				if (parameters.getMessageField() != null) {
					captions.add(parameters.getMessageField());
				}

				headers = new ReadHeaders(true, false, false, captions);
				columns = new Columns(null, headers);
			}

			target.prepareStatement(context, headers, columns);

			if (target.getParameterCount() != targetParameterCount) {
				throw new RuntimeException("Wrong number of target parameters");
			}

			// Prepare to issue HTTP requests.

			client = HttpClients.createDefault();

			// Loop over source rows.

			source.executeQuery(context);

			if (source.getColumnCount() != sourceColumnCount) {
				throw new RuntimeException("Wrong number of source columns");
			}

			while (source.next()) {

				request = buildRequest(parameters, source);

				// Execute the request and interpret the response to build the target row.

				response = client.execute(request);

				int statusCode = response.getStatusLine().getStatusCode();

				if (200 <= statusCode && statusCode < 300) {
					interpretResponse(statusCode, source, parameters, target, response);
				}
				else {
					interpretFailedResponse(statusCode, source, parameters, target, response);
				}

				target.addBatch();

				response.close();
				response = null;
			}

			target.executeBatch();

			source.done(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Web service request was terminated due to interruption");
		} catch (URISyntaxException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Invalid resolved URL: " + message);
		} catch (IOException /* catches ClientProtocolException */ ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("HTTP protocol or I/O error: " + message);
		}
		finally {
			if (response != null) try { response.close(); } catch (Exception ex) {}
			if (client != null) try { client.close(); } catch (Exception ex) {}

			target.close(context);
			source.close(context);
		}
	}

	protected int getSourceColumnCount(EvaluatedParameters parameters) {
		return parameters.getRequestFields().size();
	}

	private int getTargetParameterCount(EvaluatedParameters parameters) {
		return
				parameters.getKeepIndexes().size() +
				parameters.getResponseFields().size() +
				1 +
				(parameters.getMessageField() != null ? 1 : 0);
	}

	protected List<String> getBodyFields(EvaluatedParameters baseEvaluatedParameters) { return null; }

	private HttpRequestBase buildRequest(EvaluatedParameters parameters, DataSource source) throws SQLException, URISyntaxException {

		String resolvedUrl = parameters.getUrl();

		if (!parameters.getSubstituteIndexes().isEmpty()) {
			for (int substituteIndex : parameters.getSubstituteIndexes()) {

				String template = "{" + parameters.getRequestFields().get(substituteIndex - 1) + "}";
				String replacement = (String)source.getObject(substituteIndex);
				resolvedUrl = resolvedUrl.replace(template, replacement);
			}
		}

		HttpRequestBase request = makeRequest();

		request.setURI(new URI(resolvedUrl));

		for (EvaluatedParameters.Header header : parameters.getHeaders()) {
			request.addHeader(header.name, header.value);
		}

		addBody(request, parameters, source);

		return request;
	}

	protected abstract HttpRequestBase makeRequest();

	protected void addBody(HttpRequestBase baseRequest, EvaluatedParameters baseEvaluatedParameters, DataSource source) throws SQLException {}

	private void interpretResponse(
			int statusCode,
			DataSource source,
			EvaluatedParameters parameters,
			DataTarget target,
			CloseableHttpResponse response) throws SQLException, IllegalStateException, IOException {

		JsonReader reader = null;

		try {
			int columnIndex = keepSourceColumns(source, parameters, target);

			if (!parameters.getResponseFields().isEmpty()) {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					if (!hasSupportedContentLength (entity)) {
						throw new RuntimeException("Unknown or excessive response content length");
					}

					if (!hasSupportedContentType(entity)) {
						throw new RuntimeException("Unsupported content type in response");
					}

					reader = Json.createReader(entity.getContent());

					JsonObject responseObject = reader.readObject();
					for (String responseFieldName : parameters.getResponseFields()) {

						int parameterType = Types.NULL;
						try {
							// Some databases (MySQL) will not have metadata with parameter types available here.
							parameterType = target.getParameterType(columnIndex);
						}
						catch (SQLException ex) {}

						Object entityFieldValue = getParameterValue(responseObject, responseFieldName, parameterType);
						target.setObject(columnIndex++, entityFieldValue);
					}

					reader.close();
					reader = null;
				}
				else {
					throw new RuntimeException("Response did not include an entity as expected");
				}
			}

			target.setObject(columnIndex++, statusCode);

			if (parameters.getMessageField() != null) {
				target.setObject(columnIndex++, null);
			}
		}
		finally {
			if (reader != null) try { reader.close(); } catch (Exception ex) {}
		}
	}

	private int keepSourceColumns(
			DataSource source,
			EvaluatedParameters parameters,
			DataTarget target) throws SQLException {

		int columnIndex = 1;
		if (!parameters.getKeepIndexes().isEmpty()) {
			for (int keepIndex : parameters.getKeepIndexes()) {
				Object sourceColumnValue = source.getObject(keepIndex);
				target.setObject(columnIndex++, sourceColumnValue);
			}
		}

		return columnIndex;
	}

	private boolean hasSupportedContentLength(HttpEntity entity) {

		long contentLength = entity.getContentLength();
		return (0 <= contentLength) && (contentLength <= maxResponseContentLength);
	}

	private boolean hasSupportedContentType(HttpEntity entity) {

		ContentType contentType = ContentType.getOrDefault(entity);
		return
				contentType.getMimeType().equals(supportedContentType.getMimeType()) &&
				(contentType.getCharset() == null || contentType.getCharset().equals(supportedContentType.getCharset()));
	}

	private void interpretFailedResponse(
			int statusCode,
			DataSource source,
			EvaluatedParameters parameters,
			DataTarget target,
			CloseableHttpResponse response) throws SQLException, IllegalStateException, IOException {

		JsonReader reader = null;

		try {
			int columnIndex = keepSourceColumns(source, parameters, target);

			// Set all target columns that would come from the response to NULL.

			if (!parameters.getResponseFields().isEmpty()) {
				for (int i = 0; i < parameters.getResponseFields().size(); ++i) {
					target.setObject(columnIndex++, null);
				}
			}

			// If the response is from Jersey exception mapper, retrieve error message.
			// Otherwise, use the whole response body text as the error message,
			// regardless of the content type.

			String message = null;

			HttpEntity entity = response.getEntity();
			if (entity != null) {

				if (hasSupportedContentLength(entity) && hasSupportedContentType(entity)) {

					reader = Json.createReader(entity.getContent());

					JsonObject responseObject = reader.readObject();

					message = (String)getParameterValue(responseObject, "message", Types.VARCHAR);

					reader.close();
					reader = null;
				}
				else {
					// See http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string

					InputStream content = entity.getContent();
					String newLine = System.getProperty("line.separator");
					message = new BufferedReader(new InputStreamReader(content)).lines().collect(Collectors.joining(newLine));
				}
			}

			target.setObject(columnIndex++, statusCode);

			if (parameters.getMessageField() != null) {
				target.setObject(columnIndex++, message);
			}
		}
		finally {
			if (reader != null) try { reader.close(); } catch (Exception ex) {}
		}
	}

	/**
	 * Retrieve a field value from a JSON object converted if necessary to an object type
	 * acceptable to PreparedStatement.setObject(Object) for a specified java.sql.Types type
	 *
	 * @param object is the JSON object from which the field is retrieved
	 * @param fieldName is the name of the field to retrieve
	 * @param parameterType is the java.sql.Types type to which the returned object must be acceptable
	 * @return the value object or null if the field value is null or the field is not present in the JSON object
	 */
	private Object getParameterValue(JsonObject object, String fieldName, int parameterType) {

		JsonValue fieldValue = object.get(fieldName);
		if (fieldValue == null) {
			return null;
		}

		if (parameterType == Types.NULL) {
			// Caller could not determine the exact type needed for setObject(Object).
			// Return the actual value cast to the nearest type match.

			switch (fieldValue.getValueType()) {
			case NULL:
				return null;
			case FALSE:
				return (Boolean)false;
			case TRUE:
				return (Boolean)true;
			case NUMBER:
				parameterType = Types.DECIMAL;
				break;
			case STRING:
				parameterType = Types.VARCHAR;
				break;
			default:
				throw new RuntimeException("Unsupported value type in response");
			}
		}

		switch (parameterType) {
		case Types.TINYINT:
		case Types.INTEGER: {
			return (Integer)((JsonNumber)fieldValue).intValueExact();
		}
		case Types.BIGINT: {
			return (Long)((JsonNumber)fieldValue).longValueExact();
		}
		case Types.DECIMAL:
		case Types.NUMERIC: {
			return ((JsonNumber)fieldValue).bigDecimalValue();
		}
		case Types.DOUBLE:
		case Types.FLOAT: {
			return (Double)((JsonNumber)fieldValue).doubleValue();
		}

		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR: {
			return ((JsonString)fieldValue).getString();
		}

		case Types.DATE: {
			return LocalDate.parse(((JsonString)fieldValue).getString());
		}
		case Types.TIMESTAMP: {
			return LocalDateTime.parse(((JsonString)fieldValue).getString());
		}

		case Types.BOOLEAN: {
			switch (fieldValue.getValueType()) {
			case FALSE:
				return (Boolean)false;
			case TRUE:
				return (Boolean)true;
			case NUMBER: {
				JsonNumber numberValue = (JsonNumber)fieldValue;
				if (numberValue.isIntegral()) {
					long longValue = numberValue.longValueExact();
					if (longValue == 0) {
						return (Boolean)false;
					}
					else if (longValue == 1) {
						return (Boolean)true;
					}
				}
			}
				// Note intentional fall-through!
			default:
				throw new ClassCastException("Can't cast response value to BOOLEAN");
			}
		}
		default:
			throw new RuntimeException("Unsupported column type in target");
		}
	}
}
