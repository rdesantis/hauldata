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

package com.hauldata.ws.rs.client;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import com.hauldata.ws.rs.exception.WebException;

/**
 * Dynamically generated web client
 */
public class WebClient {

	WebTarget baseTarget;

	/**
	 * Construct a web client to access resources available from a base URL
	 *
	 * @param baseUrl is the base URL of the web resources to which the client will connect
	 */
	public WebClient(String baseUrl) {

		Client client = ClientBuilder.newClient();
		baseTarget = client.target(baseUrl);
	}

	/**
	 * Instantiate a web resource from an interface that has JAX-RS annotations
	 * <p>
	 * Only those methods in the interface annotated as GET, PUT, POST, or DELETE are implemented.
	 * For PUT and POST, it is assumed that the final parameter of each method contains the
	 * data to be posted, and all preceding parameters are annotated QueryParm or PathParam.
	 *
	 * @param clientInterface is the class of the interface for the resource to be instantiated
	 * @return a resource which can be cast to the interface being implemented and whose methods
	 * can then be called as though they were local.  Any application exception thrown on the
	 * server is mapped to RuntimeException locally.  The getMessage() method of the exception
	 * returns the message from the exception thrown on the server.
	 *
	 * @throws ReflectiveOperationException if the interface cannot be analyzed
	 */
	public Object getResource(Class<?> clientInterface) throws ReflectiveOperationException {

		InvocationHandler handler = new WebClientInvocationHandler(clientInterface, baseTarget);

		Class<?> proxyClass = Proxy.getProxyClass(clientInterface.getClassLoader(), new Class[] { clientInterface });

		return proxyClass
				.getConstructor(new Class[] { InvocationHandler.class })
				.newInstance(new Object[] { handler });
	}
}

/**
 * Invocation handler for the dynamic proxy class that implements the web client
 */
class WebClientInvocationHandler implements InvocationHandler {

	private Map<Method, WebMethod> webMethods;

	/**
	 * Constructor - builds a JAX-RS / Jersey client implementation of the interface
	 *
	 * @see WebClient#getResource(Class)
	 */
	public WebClientInvocationHandler(Class<?> clientInterface, WebTarget baseTarget) {

		// @Path("PATH")

		Annotation pathAnnotation = clientInterface.getAnnotation(Path.class);
		// Expect @javax.ws.rs.Path(value=PATH)
		String path = AnnotationParser.parseValue(pathAnnotation);
		if (path != null) {
			baseTarget = baseTarget.path(path);
		}

		// @Produces(MediaType.TYPE)

		Annotation producesAnnotation = clientInterface.getAnnotation(Produces.class);
		MediaType defaultProduces = AnnotationParser.parseMediaType(producesAnnotation, null);

		// @Consumes(MediaType.TYPE)

		Annotation consumesAnnotation = clientInterface.getAnnotation(Consumes.class);
		MediaType defaultConsumes = AnnotationParser.parseMediaType(consumesAnnotation, null);

		webMethods = new HashMap<Method, WebMethod>();
		for (Method method : clientInterface.getDeclaredMethods()) {
			WebMethod webMethod = WebMethod.of(method, baseTarget, defaultProduces, defaultConsumes);
			if (webMethod != null) {
				webMethods.put(method, webMethod);
			}
		}
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		WebMethod webMethod = webMethods.get(method);
		if (webMethod == null) {
			throw new NoSuchMethodException("Not a web client method: " + method.getName());
		}

		Object result = null;
		try {
			result = webMethod.invoke(proxy, args);
		}
		catch (WebApplicationException wax) {
			WebException entity = wax.getResponse().readEntity(WebException.class);
			throw new RuntimeException(entity.getMessage());
		}
		return result;
	}
}

/**
 * Web service client implementation of a single method of an interface
 */
abstract class WebMethod {

	/**
	 * Build a proxy class method for a web service end point
	 *
	 * @param method is the interface of a method to be implemented as a web service end point based on its JAX-RS annotations
	 * @param baseTarget is the Jersey target for the base URL of the service to which the client will connect
	 * @param defaultProduces is the media type to be produced by the method if not otherwise annotated
	 * @param defaultConsumes is the media type to be consumed by the method if not otherwise annotated
	 * @return the web service end point proxy method or null if the specified method is not annotated as a web service
	 */
	public static WebMethod of(Method method, WebTarget baseTarget, MediaType defaultProduces, MediaType defaultConsumes) {
		if (method.getAnnotation(GET.class) != null) {
			return new GetMethod(method, baseTarget, defaultProduces);
		}
		else if (method.getAnnotation(PUT.class) != null) {
			return new PutMethod(method, baseTarget, defaultProduces, defaultConsumes);
		}
		else if (method.getAnnotation(POST.class) != null) {
			return new PostMethod(method, baseTarget, defaultProduces, defaultConsumes);
		}
		else if (method.getAnnotation(DELETE.class) != null) {
			return new DeleteMethod(method, baseTarget, defaultProduces);
		}
		else {
			return null;
		}
	}

	private WebTarget target;
	private MediaType produces;
	private GenericType<?> returnType;
	private Map<String, Integer> queryParamIndexes;
	private Map<String, Integer> pathParamIndexes;

	@SuppressWarnings("rawtypes")
	protected WebMethod(Method method, WebTarget baseTarget, MediaType defaultProduces) {

		// @Path("PATH")

		target = baseTarget;

		Annotation pathAnnotation = method.getAnnotation(Path.class);
		// Expect @javax.ws.rs.Path(value=PATH)
		String path = AnnotationParser.parseValue(pathAnnotation);
		if (path != null) {
			target = baseTarget.path("/" + path);
		}

		// @Produces(MediaType.TYPE)

		Annotation producesAnnotation = method.getAnnotation(Produces.class);
		produces = AnnotationParser.parseMediaType(producesAnnotation, defaultProduces);

		returnType = new GenericType(method.getGenericReturnType());

		// Parameters

		queryParamIndexes = new HashMap<String, Integer>();
		pathParamIndexes = new HashMap<String, Integer>();

		Annotation[][] parameterAnnotations = method.getParameterAnnotations();

		int index = 0;
		for (Annotation[] annotations : parameterAnnotations) {

			String paramName;

			// @QueryParam("NAME")

			paramName = AnnotationParser.parse(annotations, QueryParam.class);
			if (paramName != null) {
				queryParamIndexes.put(paramName, index);
			}

			// @PathParam("NAME")

			paramName = AnnotationParser.parse(annotations, PathParam.class);
			if (paramName != null) {
				pathParamIndexes.put(paramName, index);
			}

			index++;
		}
	}

	protected Invocation.Builder request(Object[] args) {

		WebTarget finalTarget = target;

		if (!queryParamIndexes.isEmpty()) {

			// Add query parameters from the actual method arguments if not null.

			for (Entry<String, Integer> entry : queryParamIndexes.entrySet()) {
				Object value = args[entry.getValue()];
				if (value != null) {
					finalTarget = finalTarget.queryParam(entry.getKey(), value);
				}
			}
		}

		if (!pathParamIndexes.isEmpty()) {

			// Resolve path parameter template values from the actual method arguments.
			// Path argument values should never be null, but if so, replace with empty string.

			Map<String, Object> templateValues = new HashMap<String, Object>();

			for (Entry<String, Integer> entry : pathParamIndexes.entrySet()) {
				Object value = args[entry.getValue()];
				templateValues.put(entry.getKey(), (value != null) ? value : "");
			}

			finalTarget = target.resolveTemplates(templateValues);
		}

		return finalTarget.request(produces);
	}

	protected GenericType<?> getReturnType() {
		return returnType;
	}

	public abstract Object invoke(Object proxy, Object[] args);
}

class GetMethod extends WebMethod {

	GetMethod(Method method, WebTarget baseTarget, MediaType defaultProduces) {
		super(method, baseTarget, defaultProduces);
	}

	@Override
	public Object invoke(Object proxy, Object[] args) {
		return request(args).get(getReturnType());
	}
}

class DeleteMethod extends WebMethod {

	DeleteMethod(Method method, WebTarget baseTarget, MediaType defaultProduces) {
		super(method, baseTarget, defaultProduces);
	}

	@Override
	public Object invoke(Object proxy, Object[] args) {
		return request(args).delete(getReturnType());
	}
}

abstract class EntityMethod extends WebMethod {

	private MediaType consumes;
	private int entityIndex;

	EntityMethod(Method method, WebTarget baseTarget, MediaType defaultProduces, MediaType defaultConsumes) {
		super(method, baseTarget, defaultProduces);

		// @Consumes(MediaType.TYPE)

		Annotation consumesAnnotation = method.getAnnotation(Consumes.class);
		consumes = AnnotationParser.parseMediaType(consumesAnnotation, defaultConsumes);

		entityIndex = method.getParameterCount() - 1;
	}

	public Entity<?> getEntity(Object[] args) {
		final Boolean nothing = true;
		Object arg = (0 <= entityIndex) ? args[entityIndex] : nothing;
		return Entity.entity(arg, consumes);
	}
}

class PutMethod extends EntityMethod {

	PutMethod(Method method, WebTarget baseTarget, MediaType defaultProduces, MediaType defaultConsumes) {
		super(method, baseTarget, defaultProduces, defaultConsumes);
	}

	@Override
	public Object invoke(Object proxy, Object[] args) {
		return request(args).put(getEntity(args), getReturnType());
	}
}

class PostMethod extends EntityMethod {

	PostMethod(Method method, WebTarget baseTarget, MediaType defaultProduces, MediaType defaultConsumes) {
		super(method, baseTarget, defaultProduces, defaultConsumes);
	}

	@Override
	public Object invoke(Object proxy, Object[] args) {
		return request(args).post(getEntity(args), getReturnType());
	}
}

/**
 * Static methods for parsing values from annotations
 */
class AnnotationParser {
	/**
	 * Parse a regex group from an annotation
	 *
	 * @param annotation is the annotation to parse or null
	 * @param patternWithGroup is the pattern to match including the group to return
	 * @return the group match or null if the annotation is null or the pattern is not matched
	 */
	public static String parse(Annotation annotation, String patternWithGroup) {
		String result = null;
		if (annotation != null) {
			Matcher matcher = Pattern
					.compile(patternWithGroup)
					.matcher(annotation.toString());
			if (matcher.find()) {
				result = matcher.group(1);
			}
		}
		return result;
	}

	/**
	 * Parse the "value =" value from an annotation
	 *
	 * @param annotation is the annotation to parse or null
	 * @return the value parsed or null if the annotation is null or it does not have "value ="
	 */
	public static String parseValue(Annotation annotation) {
		return parse(annotation, "\\(value=(.+)\\)");
	}

	/**
	 * Parse media type from an annotation
	 *
	 * @param annotation is the annotation to parse or null
	 * @param defaultType is the default type
	 * @return the media type parsed or defaultType if the annotation is null or it does not match a media type
	 */
	public static MediaType parseMediaType(Annotation annotation, MediaType defaultType) {
		// Expect @javax.ws.rs.(Produces|Consumes)(value=[TYPE])
		String type = parse(annotation, "\\(value=\\[(.+)\\]\\)");
		if (type != null) {
			return MediaType.valueOf(type);
		}
		return defaultType;
	}

	/**
	 * If any of an array of annotations is of the indicated class,
	 * parse the "value =" value from the annotation.
	 *
	 * @param annotations is the array of annotations to check
	 * @param paramClass is the class of annotation to find
	 * @return the value parsed or null if no annotation is of the specified type or it does not have value
	 */
	public static String parse(Annotation[] annotations, Class<?> paramClass) {

		Annotation paramAnnotation = Stream.of(annotations)
				.filter(a -> paramClass.isInstance(a)).findFirst().orElse(null);
		return AnnotationParser.parseValue(paramAnnotation);
	}
}
