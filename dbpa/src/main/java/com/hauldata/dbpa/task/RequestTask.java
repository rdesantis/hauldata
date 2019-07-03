/*
 * Copyright (c) 2017, 2018, Ronald DeSantis
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
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.tokenizer.JsonTokenizer;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Token;
import com.hauldata.util.tokenizer.Tokenizer;
import com.hauldata.util.tokenizer.Word;

public abstract class RequestTask extends Task {

	public static final ContentType supportedContentType = ContentType.APPLICATION_JSON;

	protected Expression<String> url;
	protected Expression<Integer> connectTimeout;
	protected Expression<Integer> socketTimeout;
	protected List<Header> headers;
	protected List<SourceWithAliases> sourcesWithAliases;
	protected Expression<String> responseTemplate;
	protected List<TargetWithKeepers> targetsWithKeepers;

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

	public static class SourceWithAliases {
		public Source source;
		public List<Expression<String>> columnNameAliases;

		public SourceWithAliases(Source source, List<Expression<String>> columnNameAliases) {
			this.source = source;
			this.columnNameAliases = columnNameAliases;
		}
	}

	public static class TargetWithKeepers {
		public DataTarget target;
		public List<Expression<String>> keeperNames;

		public TargetWithKeepers(DataTarget target, List<Expression<String>> keeperNames) {
			this.keeperNames = keeperNames;
			this.target = target;
		}
	}

	protected RequestTask(
			Prologue prologue,
			Expression<String> url,
			Expression<Integer> connectTimeout,
			Expression<Integer> socketTimeout,
			List<Header> headers,
			List<SourceWithAliases> sourcesWithAliases,
			Expression<String> responseTemplate,
			List<TargetWithKeepers> targetsWithKeepers) {
		super(prologue);

		this.url = url;
		this.connectTimeout = connectTimeout;
		this.socketTimeout = socketTimeout;
		this.headers = headers;
		this.sourcesWithAliases = sourcesWithAliases;
		this.responseTemplate = responseTemplate;
		this.targetsWithKeepers = targetsWithKeepers;
	}

	@Override
	protected void execute(Context context) throws Exception {

		RequestTaskEvaluatedParameters parameters = makeParameters();

		try {
			List<List<String>> sourceColumnNames = executeSourceQueries(parameters, context);

			JsonRequestTemplate requestTemplate = prepareRequestTemplate(parameters, sourceColumnNames);

			ResponseInterpreter responseInterpreter = prepareResponseUpdates(parameters, sourceColumnNames.get(0), context);

			executeRequests(parameters, requestTemplate, responseInterpreter, context);
		}
		finally {
			parameters.close(context);
		}
	}

	/**
	 * For override by RequestWithBodyTask.
	 */
	protected RequestTaskEvaluatedParameters makeParameters() {
		return new RequestTaskEvaluatedParameters(url, connectTimeout, socketTimeout, headers, sourcesWithAliases, responseTemplate, targetsWithKeepers);
	}

	/**
	 * Run executeQuery() for the FROM and JOIN sources;
	 * sets the columnNameAliases from metadata for sources that did not explicitly specify them.
	 *
	 * @return the full set of resolved columnNameAliases as a convenience.
	 * Element 0 is guaranteed to exist; if there is no FROM clause, it will be a empty list.
	 */
	private List<List<String>> executeSourceQueries(
			RequestTaskEvaluatedParameters parameters,
			Context context) throws SQLException, InterruptedException {

		ArrayList<List<String>> sourceColumnNames = new ArrayList<List<String>>();

		if (0 < parameters.sourcesWithAliases.size()) {
			for (RequestTaskEvaluatedParameters.SourceWithAliases sourceWithAliases : parameters.sourcesWithAliases) {

				Source source = sourceWithAliases.source;
				List<String> aliases = sourceWithAliases.columnNameAliases;
				List<String> labels;

				if (source != null) {
					source.executeQuery(context);

					if ((aliases != null) && !aliases.isEmpty()) {
						if (aliases.size() != source.getColumnCount()) {
							throw new RuntimeException("AS column count does not match FROM or JOIN result set column count");
						}
						labels = aliases;
					}
					else {
						labels = new ArrayList<String>();
						for (int i = 1; i <= source.getColumnCount(); ++i) {
							String label = source.getColumnLabel(i);
							if ((label == null) || (label.length() == 0)) {
								throw new RuntimeException("FROM or JOIN result set column has no label and AS clause is not provided");
							}
							labels.add(label);
						}
						sourceWithAliases.columnNameAliases = labels;
					}
				}
				else {
					labels = new ArrayList<String>();
				}

				sourceColumnNames.add(labels);
			}

			validateJoins(sourceColumnNames, true);
		}
		else {
			sourceColumnNames.add(new ArrayList<String>());
		}

		return sourceColumnNames;
	}

	/**
	 * For override by RequestWithBodyTask.
	 */
	protected JsonRequestTemplate prepareRequestTemplate(
			RequestTaskEvaluatedParameters parameters,
			List<List<String>> sourceColumnNames) throws InputMismatchException, IOException {
		return new JsonNoBodyRequestTemplate();
	}

	/**
	 * Run prepareStatement() for the INTO and JOIN targets;
	 * parse the response template to build the response interpreter.
	 *
	 * @return the response interpreter.
	 */
	private ResponseInterpreter prepareResponseUpdates(
			RequestTaskEvaluatedParameters parameters,
			List<String> fromColumnNames,
			Context context) throws SQLException, InterruptedException, InputMismatchException, NoSuchElementException, IOException {

		for (RequestTaskEvaluatedParameters.TargetWithKeepers targetWithKeepers : parameters.targetsWithKeepers) {

			ArrayList<String> keeperNames = targetWithKeepers.keeperNames;
			DataTarget target = targetWithKeepers.target;

			if (target != null) {
				// Create Headers and Columns for use with TABLE target.
				SourceHeaders headers = new SourceHeaders(true, false, true, keeperNames);
				Columns columns = new Columns(null, headers);

				target.prepareStatement(context, headers, columns);
			}
		}

		ArrayList<List<String>> targetColumnNames = new ArrayList<List<String>>();
		for (RequestTaskEvaluatedParameters.TargetWithKeepers targetWithKeepers : parameters.targetsWithKeepers) {
			targetColumnNames.add(targetWithKeepers.keeperNames);
		}

		validateInto(targetColumnNames);
		validateJoins(targetColumnNames, false);

		return (new ResponseInterpreterBuider(fromColumnNames, parameters.responseTemplate, targetColumnNames)).build();
	}

	private void validateInto(ArrayList<List<String>> targetColumnNames) {

		if ((targetColumnNames == null) || (targetColumnNames.size() <= 1) || (targetColumnNames.get(0) == null) || (targetColumnNames.get(0).isEmpty())) {
			return;
		}

		for (String sourceLabel : targetColumnNames.get(0)) {
			if (!HttpResponseStatus.contains(sourceLabel)) {
				return;
			}
		}

		throw new RuntimeException("When using JOIN the KEEP list for the INTO clause must include at least one name other than " + HttpResponseStatus.getRendering());
	}

	private void validateJoins(ArrayList<List<String>> columnNames, boolean fromNotInto) {

		boolean hasFrom = (0 < columnNames.size()) && (columnNames.get(0) != null) && !columnNames.get(0).isEmpty();

		for (int sourceIndex = 1; sourceIndex < columnNames.size(); ++sourceIndex) {

			List<String> sourceLabels = columnNames.get(sourceIndex);
			if (hasFrom) {
				boolean hasFromColumn = false;
				for (String sourceLabel : sourceLabels) {
					if (!HttpResponseStatus.contains(sourceLabel) && columnNames.get(0).contains(sourceLabel)) {
						hasFromColumn = true;
						break;
					}
				}
				if (!hasFromColumn) {
					throw new RuntimeException(fromNotInto ?
							"The column names in a JOIN clause must include at least one column name in the FROM clause" :
							"The KEEP names in a JOIN clause must include at least one KEEP name in the INTO clause");
				}
			}

			for (int leftJoinIndex = 1; leftJoinIndex < sourceIndex; ++leftJoinIndex) {
				for (String sourceLabel : sourceLabels) {
					if (
							!HttpResponseStatus.contains(sourceLabel) && columnNames.get(leftJoinIndex).contains(sourceLabel) &&
							(!hasFrom || (hasFrom && !columnNames.get(0).contains(sourceLabel)))) {
						throw new RuntimeException(fromNotInto ?
								"The column names in a JOIN clause cannot include a column name in another JOIN clause" :
								"The KEEP names in a JOIN clause cannot include a KEEP name in another JOIN clause");
					}
				}
			}
		}
	}

	/**
	 * Execute the http request for each row of the FROM source
	 * joining to the JOIN sources; write response data to the INTO
	 * target joining to the JOIN targets according to the
	 * response interpreter.
	 */
	private void executeRequests(
			RequestTaskEvaluatedParameters parameters,
			JsonRequestTemplate requestTemplate,
			ResponseInterpreter responseInterpreter,
			Context context) throws IllegalStateException, IOException, SQLException, InterruptedException, URISyntaxException {

		CloseableHttpClient client = null;
		HttpRequestBase request = null;
		CloseableHttpResponse response = null;

		try {
			RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
			if (parameters.connectTimeout != null) {
				requestConfigBuilder = requestConfigBuilder.setConnectTimeout(parameters.connectTimeout * 1000);
			}
			if (parameters.socketTimeout != null) {
				requestConfigBuilder = requestConfigBuilder.setSocketTimeout(parameters.socketTimeout * 1000);
			}

			client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfigBuilder.build()).build();

			HttpResponseStatus finalStatus = HttpResponseStatus.OK;

			while (requestTemplate.next(parameters)) {

				String url = composeUrl(parameters.url, parameters.getSource(0), parameters.getColumnNames(0));

				String requestBody = requestTemplate.composeRequest(parameters);

				request = buildRequest(url, parameters.headers, requestBody);

				response = execute(client, request);

				HttpResponseStatus responseStatus = responseInterpreter.interpret(parameters.getSource(0), response, parameters.getTargets());

				response.close();
				response = null;

				if (!responseStatus.getIsSuccessful()) {
					finalStatus = responseStatus;
				}
			}

			parameters.done(context);

			if (!finalStatus.getIsSuccessful()) {
				throw new RuntimeException("Request failed with status code " + String.valueOf(finalStatus.getStatus()) + ": " + finalStatus.getNonNullMessage());
			}
		}
		finally {
			if (response != null) try { response.close(); } catch (Exception ex) {}
			if (client != null) try { client.close(); } catch (Exception ex) {}
		}
	}

	private String composeUrl(String urlTemplate, Source source, List<String> sourceColumnNames)
			throws SQLException, InputMismatchException, NoSuchElementException, IOException, UnsupportedEncodingException {

		// URL characters may need encoding.  It is the user's responsibility to encode any characters appearing
		// in the template itself  We are responsible for encoding substitutions into the template.
		// URLEncoder.encode() encodes query string parameters in a URL.  In concept, the path portion of a URL
		// encodes differently in that slashes in the path are not encoded.  However, we will be substituting
		// parameters into the path and we do not want slashes within a parameter to be interpreted as path
		// delimiters; therefore we encode path parameters the same as query parameters.
		// Furthermore, it is possible for the user to substitute the entire URL, not just parameters within it,
		// in which case we must leave all encoding as the user's responsibility.  We resolve this as follows.
		// If the template contains ":", we will encode all substitutions.
		// If the template does not contain ":", we will not encode any substitutions.

		Encoder encoder = urlTemplate.contains(":") ? (raw -> URLEncoder.encode(raw, "UTF-8")) : (raw -> raw);

		Tokenizer tokenizer = new Tokenizer(new StringReader(urlTemplate));
		String exceptionPrefix = "URL template error: ";
		tokenizer.exceptionMessaging(exceptionPrefix, false);

		StringBuilder result = new StringBuilder();

		while (tokenizer.hasNext()) {

			String segment;
			if (tokenizer.skipDelimiter("{")) {

				String name = tokenizer.nextWord();

				tokenizer.nextDelimiter("}");

				int columnIndex = sourceColumnNames.indexOf(name);
				if (columnIndex == -1) {
					throw new RuntimeException(exceptionPrefix + "A name in braces must be a FROM name: " + name);
				}

				Object replacement = source.getObject(columnIndex + 1);
				segment = encoder.encode(replacement.toString());
			}
			else {
				segment = tokenizer.nextToken().getImage();
			}
			result.append(segment);
		}

		return result.toString();
	}

	@FunctionalInterface
	static interface Encoder {
		String encode(String raw) throws UnsupportedEncodingException;
	}

	private HttpRequestBase buildRequest(
			String url,
			List<RequestTaskEvaluatedParameters.Header> headers,
			String requestBody) throws URISyntaxException {

		HttpRequestBase request = makeRequest();

		request.setURI(new URI(url));

		for (RequestTaskEvaluatedParameters.Header header : headers) {
			request.addHeader(header.name, header.value);
		}

		if (requestBody != null) {

			StringEntity entity = new StringEntity(requestBody, supportedContentType);

			((HttpEntityEnclosingRequestBase)request).setEntity(entity);
		}

		return request;
	}

	/**
	 * Execute HTTP request allowing long-running request to be terminated by interrupt
	 *
	 * If an InterruptedException occurs while waiting for the request to complete,
	 * the HTTP client is closed immediately and this function re-throws the InterruptedException.
	 *
	 * Note that this function spawns a worker thread to execute the HTTP request.
	 * It is assumed that closing the HTTP client on interrupt will terminate the HTTP request and thus
	 * terminate the worker thread.  However, this is not guaranteed.  In the worst case, the worker thread
	 * may continue running until a timeout is reached on the HTTP client.
	 */
	private CloseableHttpResponse execute(CloseableHttpClient client, HttpRequestBase request) throws ClientProtocolException, IOException, InterruptedException {

		HttpRequestExecutor executor = new HttpRequestExecutor(client, request);
		Thread executorThread = new Thread(executor);

		executorThread.start();

		try {
			executorThread.join();
		}
		catch (InterruptedException iex) {
			client.close();
			throw iex;
		}

		if (executor.cpex != null) { throw executor.cpex; }
		if (executor.ioex != null) { throw executor.ioex; }

		return executor.response;
	}

	private static class HttpRequestExecutor implements Runnable {
		public CloseableHttpClient client;
		public HttpRequestBase request;
		public CloseableHttpResponse response = null;
		public ClientProtocolException cpex = null;
		public IOException ioex = null;

		public HttpRequestExecutor(CloseableHttpClient client, HttpRequestBase request) {
			this.client = client;
			this.request = request;
		}

		@Override
		public void run() {
			try {
				response = client.execute(request);
			}
			catch (ClientProtocolException cpex) {
				this.cpex = cpex;
			}
			catch (IOException ioex) {
				this.ioex = ioex;
			}
		}
	}

	protected abstract HttpRequestBase makeRequest();
}

/**
 * Base class implementation for GET and DELETE
 */
class RequestTaskEvaluatedParameters {

	public String url;
	public Integer connectTimeout;
	public Integer socketTimeout;
	public List<Header> headers;
	public List<SourceWithAliases> sourcesWithAliases;
	public String responseTemplate;
	public List<TargetWithKeepers> targetsWithKeepers;

	public static class Header {
		public String name;
		public String value;

		private Header(String name, String value) { this.name = name; this.value = value; }

		public static Header evaluateHeader(RequestTask.Header header) {

			String evaluatedName = header.name.evaluate();
			String evaluatedValue = header.value.evaluate();

			if (evaluatedName == null) {
				if (header.isNullable) {
					return null;
				}
				else {
					throw new RuntimeException("Non-nullable header name evaluates to NULL");
				}
			}

			return new Header(evaluatedName, evaluatedValue);
		}
	}

	public static class SourceWithAliases {
		public Source source;
		public List<String> columnNameAliases;

		public SourceWithAliases(RequestTask.SourceWithAliases sourceWithAliases) {

			if (sourceWithAliases != null) {
				this.source = sourceWithAliases.source;

				this.columnNameAliases = new ArrayList<String>();
				if (sourceWithAliases.columnNameAliases != null) {

					for (Expression<String> columnNameAlias : sourceWithAliases.columnNameAliases) {

						String evaluatedColumnNameAlias = columnNameAlias.evaluate();
						if (this.columnNameAliases.contains(evaluatedColumnNameAlias)) {
							throw new RuntimeException("FROM or JOIN AS name is not unique: " + evaluatedColumnNameAlias);
						}
						this.columnNameAliases.add(evaluatedColumnNameAlias);
					}
				}
			}
			else {
				this.source = null;
				this.columnNameAliases = new ArrayList<String>();
			}
		}
	}

	public static class TargetWithKeepers {
		public DataTarget target;
		public ArrayList<String> keeperNames;

		public TargetWithKeepers(RequestTask.TargetWithKeepers targetWithKeepers) {

			if (targetWithKeepers != null) {
				this.target = targetWithKeepers.target;

				this.keeperNames = new ArrayList<String>();
				for (Expression<String> targetIdentifier : targetWithKeepers.keeperNames) {
					this.keeperNames.add(targetIdentifier.evaluate());
				}
			}
			else {
				this.target = null;
				this.keeperNames = new ArrayList<String>();
			}
		}
	}

	public RequestTaskEvaluatedParameters(
			Expression<String> url,
			Expression<Integer> connectTimeout,
			Expression<Integer> socketTimeout,
			List<RequestTask.Header> headers,
			List<RequestTask.SourceWithAliases> sourcesWithAliases,
			Expression<String> responseTemplate,
			List<RequestTask.TargetWithKeepers> targetsWithKeepers) {

		this.url = url.evaluate();
		if (this.url == null) {
			throw new RuntimeException("URL evaluates to NULL");
		}

		this.connectTimeout = (connectTimeout != null) ? connectTimeout.evaluate() : null;
		this.socketTimeout = (socketTimeout != null) ? socketTimeout.evaluate() : null;

		this.headers = new ArrayList<Header>();
		if (headers != null) {
			for (RequestTask.Header header : headers) {
				Header evaluatedHeader = Header.evaluateHeader(header);
				if (evaluatedHeader != null) {
					this.headers.add(evaluatedHeader);
				}
			}
		}

		this.sourcesWithAliases = new ArrayList<SourceWithAliases>();
		if (sourcesWithAliases != null) {
			for (RequestTask.SourceWithAliases sourceWithAliases : sourcesWithAliases) {
				this.sourcesWithAliases.add(new SourceWithAliases(sourceWithAliases));
			}
		}

		this.responseTemplate = (responseTemplate != null) ? responseTemplate.evaluate() : null;

		this.targetsWithKeepers = new ArrayList<TargetWithKeepers>();
		if (targetsWithKeepers != null) {
			for (RequestTask.TargetWithKeepers targetWithKeepers : targetsWithKeepers) {
				this.targetsWithKeepers.add(new TargetWithKeepers(targetWithKeepers));
			}
		}
	}

	public Source getSource(int sourceIndex) {
		return sourcesWithAliases.get(sourceIndex).source;
	}

	public List<String> getColumnNames(int sourceIndex) {
		return sourcesWithAliases.get(sourceIndex).columnNameAliases;
	}

	public ArrayList<DataTarget> getTargets() {
		return targetsWithKeepers.stream().map(t -> t.target).collect(Collectors.toCollection(ArrayList::new));
	}

	public void done(Context context) throws SQLException {

		for (SourceWithAliases sourceWithAliases : this.sourcesWithAliases) {
			if (sourceWithAliases != null && sourceWithAliases.source != null) {
				sourceWithAliases.source.done(context);
			}
		}
	}

	public void close(Context context) {

		for (SourceWithAliases sourceWithAliases : this.sourcesWithAliases) {
			if (sourceWithAliases != null && sourceWithAliases.source != null) {
				sourceWithAliases.source.close(context);
			}
		}

		for (RequestTaskEvaluatedParameters.TargetWithKeepers targetWithKeepers : this.targetsWithKeepers) {
			if (targetWithKeepers != null && targetWithKeepers.target != null) {
				targetWithKeepers.target.close(context);
			}
		}
	}
}

/**
 * Template for composing actual JSON request using source values.
 */
interface JsonRequestTemplate {
	boolean next(RequestTaskEvaluatedParameters parameters) throws SQLException, InterruptedException;
	String composeRequest(RequestTaskEvaluatedParameters parameters) throws SQLException, InterruptedException;
};

class JsonNoBodyRequestTemplate extends JsonMultiRowRequestTemplate {

	@Override
	public String composeRequest(RequestTaskEvaluatedParameters parameters) throws SQLException, InterruptedException {
		return null;
	}
}

/**
 * Object for reporting HTTP status.
 */
class HttpResponseStatus {

	public static boolean contains(String name) {
		return name.equalsIgnoreCase("STATUS") || name.equalsIgnoreCase("MESSAGE");
	}

	public static String getRendering() {
		return "STATUS or MESSAGE";
	}

	public static HttpResponseStatus OK = new HttpResponseStatus();

	public static KeepValueGetter getKeepValueGetter(String name) {
		if (name.equalsIgnoreCase("STATUS")) {
			return KeepValueGetter.STATUS;
		}
		else if (name.equalsIgnoreCase("MESSAGE")) {
			return KeepValueGetter.MESSAGE;
		}
		else {
			return null;
		}
	}

	private int status = 200;
	private String message = null;
	private boolean successful = true;

	int getStatus() { return status; }
	String getMessage() { return message; }
	String getNonNullMessage() { return (message != null) ? message : ""; }
	boolean getIsSuccessful() { return successful; }

	private HttpResponseStatus() {}

	public HttpResponseStatus(HttpResponse response) throws IllegalStateException, IOException {

		status = response.getStatusLine().getStatusCode();
		successful = (200 <= status && status < 300);
		message = null;

		if (!successful) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				if (hasSupportedContentType(entity)) {
					message = parseErrorEntity(entity);
				}
				else {
					message = getEntityContent(entity);
				}
			}
		}
	}

	public static boolean hasSupportedContentType(HttpEntity entity) {
		final ContentType supportedContentType = RequestTask.supportedContentType;

		ContentType contentType = ContentType.getOrDefault(entity);
		return
				contentType.getMimeType().equals(supportedContentType.getMimeType()) &&
				(contentType.getCharset() == null || contentType.getCharset().equals(supportedContentType.getCharset()));
	}

	public static String parseErrorEntity(HttpEntity entity) throws IllegalStateException, IOException {

		String message = null;

		JsonReader reader = null;
		try {
			reader = Json.createReader(entity.getContent());

			JsonObject responseObject = reader.readObject();

			JsonValue fieldValue = responseObject.get("message");
			if (fieldValue != null) {
				message = ((JsonString)fieldValue).getString();
			}

			reader.close();
			reader = null;
		}
		finally {
			if (reader != null) try { reader.close(); } catch (Exception ex) {}
		}

		return message;
	}

	public static String getEntityContent(HttpEntity entity) throws IllegalStateException, IOException {

		// See http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string

		InputStream content = entity.getContent();
		String newLine = System.getProperty("line.separator");
		String message = new BufferedReader(new InputStreamReader(content)).lines().collect(Collectors.joining(newLine));

		return message;
	}
}

/**
 * Object that builds a ResponseInterpreter.
 */
class ResponseInterpreterBuider {

	private List<String> fromColumnNames;
	private String responseTemplate;
	private ArrayList<List<String>> targetColumnNames;

	private JsonTokenizer tokenizer = null;
	private final String exceptionPrefix = "Response template error: ";

	private Set<JsonValueNode> intoValueReferences = new HashSet<JsonValueNode>();
	private ArrayList<JoinReference> joinReferences = new ArrayList<JoinReference>();

	private static class JoinReference {
		public int targetIndex;
		public Set<JsonValueNode> joinValueReferences = new HashSet<JsonValueNode>();

		public JoinReference(int targetIndex, Set<JsonValueNode> joinValueReferences) {
			this.targetIndex = targetIndex;
			this.joinValueReferences = joinValueReferences;
		}
	}

	ResponseInterpreterBuider(List<String> fromColumnNames, String responseTemplate, ArrayList<List<String>> targetColumnNames) {
		this.fromColumnNames = fromColumnNames;
		this.responseTemplate = responseTemplate;
		this.targetColumnNames = targetColumnNames;
	}

	public ResponseInterpreter build() throws InputMismatchException, NoSuchElementException, IOException {

		if ((responseTemplate == null) || responseTemplate.isEmpty() || (targetColumnNames == null) || targetColumnNames.isEmpty()) {
			return ResponseInterpreter.NULL;
		}

		tokenizer = new JsonTokenizer(new StringReader(responseTemplate));
		tokenizer.exceptionMessaging(exceptionPrefix, false);
		tokenizer.useDelimiter("...");

		JsonResponseTemplateNode root = null;

		boolean isResponseText = false;
		try {
			if (tokenizer.hasNextQuoted()) {
				isResponseText = true;
				root = parseQuoted();
			}
			else if (tokenizer.hasNextDelimiter("[")) {
				root = parseArray(intoValueReferences).node;
			}
			else if (tokenizer.hasNextDelimiter("{")) {
				root = parseObject(intoValueReferences).node;
			}
			else {
				throw new RuntimeException(exceptionPrefix + "not a valid JSON template");
			}
		}
		finally {
			if (tokenizer != null) try { tokenizer.close(); } catch (Exception ex) {}
		}

		return makeResponseInterpreter(root, isResponseText);
	}

	private ResponseInterpreter makeResponseInterpreter(JsonResponseTemplateNode root, boolean isResponseText) {

		ArrayList<ArrayList<KeepValueGetter>> keepValueGetters = new ArrayList<ArrayList<KeepValueGetter>>();

		ArrayList<KeepValueGetter> intoKeepValueGetters = new ArrayList<KeepValueGetter>();
		for (int columnIndex = 0; columnIndex < targetColumnNames.get(0).size(); ++columnIndex) {
			String name = targetColumnNames.get(0).get(columnIndex);
			intoKeepValueGetters.add(getKeepValueGetter(name, 0, columnIndex));
		}
		keepValueGetters.add(intoKeepValueGetters);

		alignJoinReferences();

		for (int targetIndex = 1; targetIndex < targetColumnNames.size(); ++targetIndex) {

			ArrayList<KeepValueGetter> joinKeepValuesGetters = new ArrayList<KeepValueGetter>();
			for (int columnIndex = 0; columnIndex < targetColumnNames.get(targetIndex).size(); ++columnIndex) {
				String name = targetColumnNames.get(targetIndex).get(columnIndex);
				joinKeepValuesGetters.add(getKeepValueGetter(name, targetIndex, columnIndex));
			}

			keepValueGetters.add(joinKeepValuesGetters);
		}

		return new ResponseInterpreter(root, isResponseText, keepValueGetters);
	}

	/**
	 * Sort the joinGetters by targetIndex and then add a null element the head so that
	 * the index of each element within joinGetters matches its targetIndex.
	 *
	 * This is required so that getKeepValueGetter(String, int sourceIndex, int) will work properly
	 * for join references, i.e., where 0 < sourceIndex.
	 */
	private void alignJoinReferences() {

		Collections.sort(joinReferences, (a, b) -> Integer.compare(a.targetIndex, b.targetIndex));
		joinReferences.add(0, null);

		if (joinReferences.size() != targetColumnNames.size()) {
			throw new RuntimeException(exceptionPrefix + "Must include exactly one dynamic structure for each JOIN clause");
		}
	}

	private KeepValueGetter getKeepValueGetter(String name, int sourceIndex, int columnIndex) {

		// See if the name is an HTTP status field.

		KeepValueGetter getter = HttpResponseStatus.getKeepValueGetter(name);
		if (getter != null) {
			return getter;
		}

		// See if the name is a FROM column.

		int fromColumnIndex = fromColumnNames.indexOf(name);
		if (fromColumnIndex != -1) {
			return new KeepFromValueGetter(fromColumnIndex);
		}

		// See if the name references a value in the JSON response.

		if (sourceIndex == 0 ?
				intoValueReferences.stream().anyMatch(n -> (n.columnIndex == columnIndex)) :
				joinReferences.get(sourceIndex).joinValueReferences.stream().anyMatch(n -> (n.columnIndex == columnIndex))) {
			return new KeepResponseValueGetter(columnIndex);
		}

		throw new RuntimeException(exceptionPrefix + "Unrecognized KEEP name: " + name);
	}

	/**
	 * Parse a response template that consists of only a quoted identifier.
	 */
	private JsonResponseTemplateNode parseQuoted() throws InputMismatchException, NoSuchElementException, IOException {

		Tokenizer innerTokenizer = null;
		try {
			String responseBody = tokenizer.nextQuoted().getBody();
			if (tokenizer.hasNext()) {
				throw new RuntimeException(exceptionPrefix + "Unexpected token: " + tokenizer.nextToken().getImage());
			}

			innerTokenizer = new Tokenizer(new StringReader(responseBody));
			innerTokenizer.exceptionMessaging(exceptionPrefix, false);

			String name = innerTokenizer.nextWord();
			if (innerTokenizer.hasNext()) {
				throw new RuntimeException(exceptionPrefix + "Unexpected token in quoted response template: " + tokenizer.nextToken().getImage());
			}

			return addNameNode(name, intoValueReferences);
		}
		finally {
			if (innerTokenizer != null) try { innerTokenizer.close(); } catch (Exception ex) {}
		}
	}

	/**
	 * Parse an array.
	 *
	 * @return a NodeIsDynamic object containing the parsed array with the isDynamic field set true
	 * if the array is dynamic or it contains a dynamic element.
	 */
	private NodeIsDynamic parseArray(Set<JsonValueNode> valueReferences) throws InputMismatchException, NoSuchElementException, IOException {

		JsonArrayNode result = null;

		Set<JsonValueNode> potentialJoinValueReferences = new HashSet<JsonValueNode>();

		tokenizer.nextDelimiter("[");

		NodeIsDynamic value = parseElement(potentialJoinValueReferences);
		boolean containsDynamic = value.isDynamic;

		boolean isDynamic = false;
		boolean hasMoreElements = false;

		if ((hasMoreElements = tokenizer.skipDelimiter(","))) {
			if ((isDynamic = tokenizer.hasNextDelimiter("..."))) {
				if (containsDynamic) {
					throw new RuntimeException(exceptionPrefix + "Dynamic structures cannot be nested");
				}
				else {
					result = addDynamicArray(value.node, potentialJoinValueReferences);

					tokenizer.nextToken();

					hasMoreElements = false;
				}
			}
		}

		if (!isDynamic) {
			merge(valueReferences, potentialJoinValueReferences);

			result = new JsonArrayNode();

			result.nodes.add(value.node);

			if (hasMoreElements) {
				do {
					value = parseElement(valueReferences);

					if (value.isDynamic) {
						containsDynamic = true;
					}

					result.nodes.add(value.node);
				} while (tokenizer.skipDelimiter(","));
			}
		}

		tokenizer.nextDelimiter("]");

		return new NodeIsDynamic(result, isDynamic || containsDynamic);
	}

	/**
	 * Parse an object.
	 *
	 * @return a NodeIsDynamic object containing the parsed object with the isDynamic field set true
	 * if the object is dynamic or it contains a dynamic element.
	 */
	private NodeIsDynamic parseObject(Set<JsonValueNode> valueReferences) throws InputMismatchException, NoSuchElementException, IOException {

		JsonObjectNode result = null;

		Set<JsonValueNode> potentialJoinValueReferences = new HashSet<JsonValueNode>();

		tokenizer.nextDelimiter("{");

		boolean isKeyDynamic = hasDynamicKey(tokenizer);
		Token keyToken = tokenizer.nextToken();

		tokenizer.nextDelimiter(":");

		NodeIsDynamic value = parseElement(potentialJoinValueReferences);
		boolean containsDynamic = value.isDynamic;

		boolean isDynamic = false;
		boolean hasMoreElements = false;

		if ((hasMoreElements = tokenizer.skipDelimiter(","))) {
			if ((isDynamic = tokenizer.hasNextDelimiter("..."))) {
				if (containsDynamic) {
					throw new RuntimeException(exceptionPrefix + "Dynamic structures cannot be nested");
				}
				else if (!isKeyDynamic) {
					throw new RuntimeException(exceptionPrefix + "A dynamic object must start with a KEEP name from a JOIN clause");
				}
				else {
					result = addDynamicObject(makeDynamicKey(keyToken, potentialJoinValueReferences), value.node, potentialJoinValueReferences);

					tokenizer.nextToken();

					hasMoreElements = false;
				}
			}
		}

		if (!isDynamic) {
			if (isKeyDynamic) {
				throw new RuntimeException(exceptionPrefix + "An object that starts with a KEEP name from a JOIN clause must be a dynamic object");
			}
			else {
				merge(valueReferences, potentialJoinValueReferences);

				result = new JsonObjectNode();

				result.nodes.put(makeStaticKey(keyToken), value.node);

				if (hasMoreElements) {
					do {
						String key = tokenizer.nextQuoted().getImage();
						tokenizer.nextDelimiter(":");
						value = parseElement(valueReferences);

						if (value.isDynamic) {
							containsDynamic = true;
						}

						result.nodes.put(key, value.node);
					} while (tokenizer.skipDelimiter(","));
				}
			}
		}

		tokenizer.nextDelimiter("}");

		return new NodeIsDynamic(result, isDynamic || containsDynamic);
	}

	/**
	 * @return true if the next token is a name.
	 * @throws RunTime exception if the next token is not a valid key, i.e., neither a name nor a quoted string
	 */
	private boolean hasDynamicKey(JsonTokenizer tokenizer) throws IOException {
		boolean hasWord = tokenizer.hasNextWord();
		if (!hasWord && !tokenizer.hasNextQuoted()) {
			throw new RuntimeException(exceptionPrefix + "Invalid key for an object field: " + tokenizer.nextToken().getImage());
		}
		return hasWord;
	}

	/**
	 * @param token must be a token parsed after hasDynamicKey(JsonTokenizer) returned true.
	 */
	private JsonValueNode makeDynamicKey(Token token, Set<JsonValueNode> valueReferences) {
		return addNameNode(((Word)token).getImage(), valueReferences);
	}

	/**
	 * @param token must be a token parsed after hasDynamicKey(JsonTokenizer) returned false.
	 */
	private String makeStaticKey(Token token) {
		return ((Quoted)token).getImage();
	}

	/**
	 * Merge the wrongly-diagnosed potential dynamic element references into the enclosing section getters.
	 */
	private void merge(Set<JsonValueNode> valueGetters, Set<JsonValueNode> potentialJoinValueReferences) {

		if (potentialJoinValueReferences.isEmpty()) {
			return;
		}
		else if (!valueGetters.isEmpty() && (getTargetIndex(valueGetters) != getTargetIndex(potentialJoinValueReferences))) {
			JsonValueNode valueGetter = potentialJoinValueReferences.iterator().next();
			String name = targetColumnNames.get(valueGetter.targetIndex).get(valueGetter.columnIndex);
			throw new RuntimeException(exceptionPrefix + "A KEEP name must appear in the same same dynamic or non-dynamic element as its peers: " + name);
		}
		else {
			valueGetters.addAll(potentialJoinValueReferences);
		}
	}

	/**
	 * Parse an element.
	 */
	private NodeIsDynamic parseElement(Set<JsonValueNode> valueReferences) throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.hasNextDelimiter("[")) {
			return parseArray(valueReferences);
		}
		else if (tokenizer.hasNextDelimiter("{")) {
			return parseObject(valueReferences);
		}
		else if (tokenizer.hasNextWord()) {
			return new NodeIsDynamic(addNameNode(tokenizer.nextWord(), valueReferences), false);
		}
		else {
			tokenizer.nextToken();
			return NodeIsDynamic.FALSE;
		}
	}

	/**
	 * Add a dynamic array to the joinReferences.
	 */
	private JsonDynamicArrayNode addDynamicArray(JsonResponseTemplateNode element, Set<JsonValueNode> joinValueReferences) {

		int targetIndex = getJoinIndex(joinValueReferences);

		joinReferences.add(new JoinReference(targetIndex, joinValueReferences));

		return new JsonDynamicArrayNode(targetIndex, element);
	}

	/**
	 * Add a dynamic object to the joinReferences.
	 */
	private JsonDynamicObjectNode addDynamicObject(JsonValueNode key, JsonResponseTemplateNode value, Set<JsonValueNode> joinValueReferences) {

		int targetIndex = getJoinIndex(joinValueReferences);

		joinReferences.add(new JoinReference(targetIndex, joinValueReferences));

		return new JsonDynamicObjectNode(targetIndex, key, value);
	}

	/**
	 * Return the index within targetColumnNames of the KEEP JOIN list referred to by valueGetters.
	 *
	 * @param valueReferences contains the references to check.
	 * @return the index.  Note that the first join is at index 1.
	 *
	 * @throws RuntimeException if valueGetters is empty or if it refers to the KEEP INTO list.
	 */
	private int getJoinIndex(Set<JsonValueNode> valueReferences) {

		int targetIndex = getTargetIndex(valueReferences);

		if (targetIndex == -1) {
			throw new RuntimeException(exceptionPrefix + "A dynamic structure must reference at least one KEEP name from a JOIN clause");
		}
		else if (targetIndex == 0) {
			throw new RuntimeException(exceptionPrefix + "A KEEP name from the INTO clause cannot appear in a dynamic object");
		}

		return targetIndex;
	}

	/**
	 * Add node for the value of the name and add to the valueGetters.
	 */
	private JsonValueNode addNameNode(String name, Set<JsonValueNode> valueReferences) {

		int targetIndex;
		int columnIndex = -1;

		for (targetIndex = 0; targetIndex < targetColumnNames.size(); ++targetIndex) {
			if ((columnIndex = targetColumnNames.get(targetIndex).indexOf(name)) != -1) {
				break;
			}
		}

		if (columnIndex == -1) {
			throw new RuntimeException(exceptionPrefix + "An unquoted field name must be a KEEP name: " + name);
		}

		int targetIndexSoFar = getTargetIndex(valueReferences);
		if ((targetIndexSoFar != -1) && (targetIndex != targetIndexSoFar)) {
			throw new RuntimeException(exceptionPrefix + "A KEEP name must appear in the same dynamic or non-dynamic element as its peers: " + name);
		}

		JsonValueNode node = new JsonValueNode(targetIndex, columnIndex);

		boolean isAdded = valueReferences.add(node);

		if (!isAdded) {
			throw new RuntimeException(exceptionPrefix + "A KEEP name cannot appear more than once: " + name);
		}

		return node;
	}

	/**
	 * Return the index within targetColumnNames of the KEEP list referred to by valueGetters.
	 *
	 * @param valueReferences contains the references to check.
	 * @return the index or -1 if joinValueGetters is empty.
	 */
	private int getTargetIndex(Set<JsonValueNode> valueReferences) {

		Iterator<JsonValueNode> iterator = valueReferences.iterator();
		if (!iterator.hasNext()) {
			return -1;
		}
		else {
			return iterator.next().targetIndex;
		}
	}
}

/**
 * Node with indicator of whether the node itself is dynamic or it contains a dynamic node.
 */
class NodeIsDynamic {
	public JsonResponseTemplateNode node;
	public boolean isDynamic;

	NodeIsDynamic(JsonResponseTemplateNode node, boolean isDynamic) {
		this.node = node;
		this.isDynamic = isDynamic;
	}

	public static final NodeIsDynamic FALSE = new NodeIsDynamic(null, false);
}

class KeyIsDynamic {
	public String key;
	public boolean isKeyDynamic;

	KeyIsDynamic(String key, boolean keyIsDynamic) {
		this.key = key;
		this.isKeyDynamic = keyIsDynamic;
	}
}

class KeyValueIsDynamic extends KeyIsDynamic {
	public JsonResponseTemplateNode node;
	public boolean isValueDynamic;

	KeyValueIsDynamic(String key, boolean isKeyDynamic, JsonResponseTemplateNode node, boolean isValueDynamic) {
		super(key, isKeyDynamic);
		this.node = node;
		this.isValueDynamic = isValueDynamic;
	}

	KeyValueIsDynamic(KeyIsDynamic keyIsDynamic, NodeIsDynamic valueIsDynamic) {
		this(keyIsDynamic.key, keyIsDynamic.isKeyDynamic, valueIsDynamic.node, valueIsDynamic.isDynamic);
	}
}

/**
 * Object that knows how to retrieve the value of all KEEP FROM and KEEP JOIN fields associated with a response.
 */
class ResponseInterpreter {
	public JsonResponseTemplateNode root;
	public boolean isResponseText;
	public ArrayList<ArrayList<KeepValueGetter>> keepValueGetters;

	public static final ResponseInterpreter NULL = new ResponseInterpreter() {
		public HttpResponseStatus interpret(Source fromSource, CloseableHttpResponse response, ArrayList<DataTarget> targets) { return HttpResponseStatus.OK; } };

	private ResponseInterpreter() {}

	public ResponseInterpreter(JsonResponseTemplateNode root, boolean isResponseText, ArrayList<ArrayList<KeepValueGetter>> keepValueGetters) {
		this.root = root;
		this.isResponseText = isResponseText;
		this.keepValueGetters = keepValueGetters;
	}

	public HttpResponseStatus interpret(Source fromSource, CloseableHttpResponse response, ArrayList<DataTarget> targets)
			throws IllegalStateException, IOException, SQLException, InterruptedException {
		return (new ResponseReader(fromSource, root, isResponseText, keepValueGetters, response, targets)).read();
	}
}

/**
 * Object that parses an actual response and sets the value of all KEEP FROM and KEEP JOIN fields associated with it.
 */
class ResponseReader {
	private Source fromSource;
	private JsonResponseTemplateNode root;
	private boolean isResponseText;
	private ArrayList<ArrayList<KeepValueGetter>> keepValueGetters;
	private CloseableHttpResponse response;
	private ArrayList<DataTarget> targets;

	private HttpResponseStatus responseStatus;
	private Object[] intoValues;
	private ArrayList<Object[]>[] joinValues;

	private JsonTokenizer tokenizer = null;
	private final String exceptionPrefix = "Response error: ";

	public ResponseReader(
			Source fromSource,
			JsonResponseTemplateNode root,
			boolean isResponseText,
			ArrayList<ArrayList<KeepValueGetter>> keepValueGetters,
			CloseableHttpResponse response,
			ArrayList<DataTarget> targets) {

		this.fromSource = fromSource;
		this.root = root;
		this.isResponseText = isResponseText;
		this.keepValueGetters = keepValueGetters;
		this.response = response;
		this.targets = targets;

		intoValues = new Object[keepValueGetters.get(0).size()];
		Arrays.fill(intoValues, null);

		joinValues = newJoinValues();
		for (int targetIndex = 1; targetIndex < keepValueGetters.size(); ++targetIndex) {
			joinValues[targetIndex] = new ArrayList<Object[]>();
		}
	}

	@SuppressWarnings("unchecked")
	private ArrayList<Object[]>[] newJoinValues() {
		return new ArrayList[keepValueGetters.size()];
	}

	public HttpResponseStatus read() throws IllegalStateException, IOException, SQLException, InterruptedException {

		responseStatus = new HttpResponseStatus(response);

		if (responseStatus.getIsSuccessful()) {
			if (isResponseText) {
				getTextValue();
			}
			else {
				parseValues();
			}
		}

		setValues();

		return responseStatus;
	}

	private void parseValues() throws IllegalStateException, IOException {

		try {
			HttpEntity entity = response.getEntity();

			InputStream content = entity.getContent();

			tokenizer = new JsonTokenizer(new InputStreamReader(content));

			parseStructure(root, intoValues);
		}
		finally {
			if (tokenizer != null) try { tokenizer.close(); } catch (Exception ex) {}
		}
	}

	private void parseStructure(JsonResponseTemplateNode node, Object[] values) throws InputMismatchException, IOException {

		if (tokenizer.hasNextDelimiter("[")) {
			parseArray(node, values);
		}
		else if (tokenizer.hasNextDelimiter("{")) {
			parseObject(node, values);
		}
		else {
			throw new RuntimeException(exceptionPrefix + "The response is not valid JSON");
		}
	}

	private void parseArray(JsonResponseTemplateNode node, Object[] values) throws InputMismatchException, NoSuchElementException, IOException {

		if (!node.isArray()) {
			throw new RuntimeException(exceptionPrefix + "The response contains an array where not expected");
		}

		tokenizer.nextDelimiter("[");

		if (!tokenizer.hasNextDelimiter("]")) {
			if (node.isDynamic()) {
				parseDynamicArray((JsonDynamicArrayNode)node);
			}
			else {
				ArrayList<JsonResponseTemplateNode> elementNodes = ((JsonArrayNode)node).nodes;

				int index = 0;
				do {
					if ((index < elementNodes.size()) && (elementNodes.get(index) != null)) {
						parseElement(elementNodes.get(index), values);
					}
					else {
						skipElement();
					}
				} while (tokenizer.skipDelimiter(","));
			}
		}

		tokenizer.nextDelimiter("]");
	}

	private void parseDynamicArray(JsonDynamicArrayNode node) throws InputMismatchException, IOException {

		do {
			Object[] values = new Object[keepValueGetters.get(node.targetIndex).size()];
			Arrays.fill(values, null);

			joinValues[node.targetIndex].add(values);

			parseElement(node.element, values);
		} while (tokenizer.skipDelimiter(","));
	}

	private void parseObject(JsonResponseTemplateNode node, Object[] values) throws InputMismatchException, NoSuchElementException, IOException {

		if (!node.isObject()) {
			throw new RuntimeException(exceptionPrefix + "The response contains an object where not expected");
		}

		tokenizer.nextDelimiter("{");

		if (!tokenizer.hasNextDelimiter("}")) {
			if (node.isDynamic()) {
				parseDynamicObject((JsonDynamicObjectNode)node);
			}
			else {
				Map<String, JsonResponseTemplateNode> fieldNodes = ((JsonObjectNode)node).nodes;

				do {
					String fieldName = tokenizer.nextQuoted().getImage();
					JsonResponseTemplateNode fieldNode = fieldNodes.get(fieldName);

					tokenizer.nextDelimiter(":");

					if (fieldNode != null) {
						parseElement(fieldNode, values);
					}
					else {
						skipElement();
					}
				} while (tokenizer.skipDelimiter(","));
			}
		}

		tokenizer.nextDelimiter("}");
	}

	private void parseDynamicObject(JsonDynamicObjectNode node) throws InputMismatchException, NoSuchElementException, IOException {

		do {
			Object[] values = new Object[keepValueGetters.get(node.targetIndex).size()];
			Arrays.fill(values, null);

			joinValues[node.targetIndex].add(values);

			parseElement(node.key, values);

			tokenizer.nextDelimiter(":");

			parseElement(node.value, values);
		} while (tokenizer.skipDelimiter(","));
	}

	private void parseElement(JsonResponseTemplateNode node, Object[] values) throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.hasNextDelimiter("[")) {
			parseArray(node, values);
		}
		else if (tokenizer.hasNextDelimiter("{")) {
			parseObject(node, values);
		}
		else {
			parseValue(node, values);
		}
	}

	private void parseValue(JsonResponseTemplateNode node, Object[] values) throws InputMismatchException, NoSuchElementException, IOException {

		if (!node.isValue()) {
			throw new RuntimeException(exceptionPrefix + "Unexpected token: " + tokenizer.nextToken().getImage());
		}

		Object value;
		if (tokenizer.hasNextQuoted()) {
			value = tokenizer.nextQuoted().getBody();
		}
		else if (tokenizer.hasNextWord()) {
			String word = tokenizer.nextWord();
			if (word.equals("true")) {
				value = Boolean.TRUE;
			}
			else if (word.equals("false")) {
				value = Boolean.FALSE;
			}
			else if (word.equals("null")) {
				value = null;
			}
			else {
				throw new RuntimeException(exceptionPrefix + "Invalid value word: " + word);
			}
		}
		else {
			Token token = tokenizer.nextToken();
			if (token.getValue() instanceof Number) {
				value = token.getValue();
			}
			else {
				throw new RuntimeException("Unrecognized value token: " + token.getImage());
			}
		}

		values[((JsonValueNode)node).columnIndex] = value;
	}

	private void skipElement() throws InputMismatchException, IOException {

		if (tokenizer.hasNextDelimiter("[")) {
			skipStructure("[", "]");
		}
		else if (tokenizer.hasNextDelimiter("{")) {
			skipStructure("{", "}");
		}
		else {
			tokenizer.nextToken();
		}
	}

	private void skipStructure(String opener, String closer) throws InputMismatchException, NoSuchElementException, IOException {

		tokenizer.skipDelimiter(opener);
		while (!tokenizer.hasNextDelimiter(closer)) {
			skipElement();
		}
		tokenizer.nextDelimiter(closer);
	}

	private void getTextValue() throws IllegalStateException, IOException {

		HttpEntity entity = response.getEntity();

		String text = HttpResponseStatus.getEntityContent(entity);

		intoValues[((JsonValueNode)root).columnIndex] = text;
	}

	private void setValues() throws SQLException, InterruptedException {

		if ((targets == null) || (targets.isEmpty())) {
			return;
		}

		if (targets.get(0) != null) {
			DataTarget intoTarget = targets.get(0);
			KeepSources keepFromSources = new KeepSources(fromSource, responseStatus, intoValues);

			int columnIndex = 1;
			for (KeepValueGetter intoValueGetter : keepValueGetters.get(0)) {
				intoTarget.setObject(columnIndex++, intoValueGetter.getValue(keepFromSources));
			}
			intoTarget.addBatch();
			intoTarget.executeBatch();
		}

		for (int targetIndex = 1; targetIndex < keepValueGetters.size(); ++targetIndex) {
			DataTarget joinTarget = targets.get(targetIndex);

			for  (Object[] joinRowValues : joinValues[targetIndex]) {
				KeepSources keepJoinSources = new KeepSources(fromSource, responseStatus, joinRowValues);

				int columnIndex = 1;
				for (KeepValueGetter joinValueGetter : keepValueGetters.get(targetIndex)) {
					joinTarget.setObject(columnIndex++, joinValueGetter.getValue(keepJoinSources));
				}
				joinTarget.addBatch();
			}
			joinTarget.executeBatch();
		}
	}
}

/**
 * Object that can retrieve the value of a KEEP field.
 */
abstract class KeepValueGetter {
	abstract Object getValue(KeepSources sources) throws SQLException;

	static final KeepValueGetter STATUS = new KeepValueGetter() {
		@Override
		public Object getValue(KeepSources sources) {
			return sources.responseStatus.getStatus();
		}
	};

	static final KeepValueGetter MESSAGE = new KeepValueGetter() {
		@Override
		public Object getValue(KeepSources sources) {
			return sources.responseStatus.getMessage();
		}
	};
}

class KeepFromValueGetter extends KeepValueGetter {
	private int dbColumnIndex;

	public KeepFromValueGetter(int columnIndex) {
		this.dbColumnIndex = columnIndex + 1;
	}

	@Override
	public Object getValue(KeepSources sources) throws SQLException {
		return sources.fromSource.getObject(dbColumnIndex);
	}
}

class KeepResponseValueGetter extends KeepValueGetter {
	private int columnIndex;

	public KeepResponseValueGetter(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	@Override
	public Object getValue(KeepSources sources) {
		return sources.values[columnIndex];
	}
}

/**
 * The set of sources from which KEEP values can be retrieved.
 */
class KeepSources {
	public Source fromSource;
	public HttpResponseStatus responseStatus;
	public Object[] values;

	public KeepSources(Source fromSource, HttpResponseStatus responseStatus, Object[] values) {
		this.fromSource = fromSource;
		this.responseStatus = responseStatus;
		this.values = values;
	}
}

/**
 * Node in the parse tree of a JSON response template.
 */
interface JsonResponseTemplateNode {
	default boolean isValue() { return false; }
	default boolean isArray() { return false; }
	default boolean isObject() { return false; }
	default boolean isDynamic() { return false; }
}

class JsonValueNode implements JsonResponseTemplateNode {
	public int targetIndex;
	public int columnIndex;

	public JsonValueNode(int targetIndex, int columnIndex) {
		this.targetIndex = targetIndex;
		this.columnIndex = columnIndex;
	}

	@Override
	public boolean isValue() { return true; }

	@Override
	public int hashCode() {
		return targetIndex << 16 + columnIndex;
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof JsonValueNode) && (((JsonValueNode)obj).targetIndex == targetIndex) && (((JsonValueNode)obj).columnIndex == columnIndex);
	}
}

class JsonArrayNode implements JsonResponseTemplateNode {
	public ArrayList<JsonResponseTemplateNode> nodes = new ArrayList<JsonResponseTemplateNode>();

	@Override
	public boolean isArray() { return true; }
}

class JsonObjectNode implements JsonResponseTemplateNode {
	public Map<String, JsonResponseTemplateNode> nodes = new HashMap<String, JsonResponseTemplateNode>();

	@Override
	public boolean isObject() { return true; }
}

interface JsonDynamicNode extends JsonResponseTemplateNode {
	int getTargetIndex();
}

class JsonDynamicArrayNode extends JsonArrayNode implements JsonDynamicNode {
	public int targetIndex;
	public JsonResponseTemplateNode element;

	public JsonDynamicArrayNode(int targetIndex, JsonResponseTemplateNode element) {
		this.targetIndex = targetIndex;
		this.element = element;
	}

	@Override
	public int getTargetIndex() { return targetIndex; }

	@Override
	public boolean isDynamic() { return true; }
}

class JsonDynamicObjectNode extends JsonObjectNode implements JsonDynamicNode {
	public int targetIndex;
	JsonValueNode key;
	JsonResponseTemplateNode value;

	public JsonDynamicObjectNode(int targetIndex, JsonValueNode key, JsonResponseTemplateNode value) {
		this.targetIndex = targetIndex;
		this.key = key;
		this.value = value;
	}

	@Override
	public int getTargetIndex() { return targetIndex; }

	@Override
	public boolean isDynamic() { return true; }
}
