/*
 * Copyright (c) 2016, Ronald DeSantis
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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

/**
 * Dynamically generated web client
 */
public class WebClient {

	/**
	 * Instantiate web client for an interface having JAX-RS annotations
	 *
	 * @param clientInterface is class of the interface for which a web client is instantiated
	 * @param baseUrl is the base URL of the web service to which the client will connect
	 * @return the web client instance which can be cast to the interface being implemented
	 * @throws ReflectiveOperationException
	 */
	public static Object of(Class<?> clientInterface, String baseUrl) throws ReflectiveOperationException {

		InvocationHandler handler = new WebClientInvocationHandler(clientInterface, baseUrl);

		Class<?> proxyClass = Proxy.getProxyClass(clientInterface.getClassLoader(), new Class[] { clientInterface });

		return proxyClass
				.getConstructor(new Class[] { InvocationHandler.class })
				.newInstance(new Object[] { handler });
	}
}

/**
 * Static methods to help build web client requests
 */
class WebClientHelper {
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
	 * Parse the "value =" value from the annotation
	 *
	 * @param annotation is the annotation to parse or null
	 * @return the value parsed or null if the annotation is null or it does not have value
	 */
	public static String parseValue(Annotation annotation) {
		return parse(annotation, "\\(value=(.+)\\)");
	}

	/**
	 * Parse media type from the annotation
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
		return WebClientHelper.parseValue(paramAnnotation);
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
	 * @see WebClient#of(Class, String)
	 */
	public WebClientInvocationHandler(Class<?> clientInterface, String baseUrl) {

		Client client = ClientBuilder.newClient();
		WebTarget baseTarget = client.target(baseUrl);

		// @Path("PATH")

		Annotation pathAnnotation = clientInterface.getAnnotation(Path.class);
		// Expect @javax.ws.rs.Path(value=PATH)
		String path = WebClientHelper.parseValue(pathAnnotation);
		if (path != null) {
			baseTarget = baseTarget.path(path);
		}

		// @Produces(MediaType.TYPE)

		Annotation producesAnnotation = clientInterface.getAnnotation(Produces.class);
		MediaType defaultProduces = WebClientHelper.parseMediaType(producesAnnotation, null);

		// @Consumes(MediaType.TYPE)

		Annotation consumesAnnotation = clientInterface.getAnnotation(Consumes.class);
		MediaType defaultConsumes = WebClientHelper.parseMediaType(consumesAnnotation, null);

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
		return webMethod.invoke(proxy, args);
	}
}

/**
 * Web service client implementation of a single method of an interface
 */
abstract class WebMethod {

	/**
	 * Build a proxy class method for the web service client
	 *
	 * @param method is the interface of a method to be implemented as a web service method based on its JAX-RS annotations
	 * @param baseTarget is the Jersey target for the base URL of the service to which the client will connect
	 * @param defaultProduces is the media type to be produced by the method if not otherwise annotated
	 * @param defaultConsumes is the media type to be consumed by the method if not otherwise annotated
	 * @return the web service method proxy or null if the specified method is not annotated as a web service
	 */
	public static WebMethod of(Method method, WebTarget baseTarget, MediaType defaultProduces, MediaType defaultConsumes) {
		if (method.getAnnotation(GET.class) != null) {
			return new GetMethod(method, baseTarget, defaultProduces);
		}
		else if (method.getAnnotation(PUT.class) != null) {
			return new PutMethod(method, baseTarget, defaultProduces, defaultConsumes);
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
	private Class<?> returnType;
	private Map<String, Integer> templateIndexes;
	private Map<String, Object> templateValues;

	protected WebMethod(Method method, WebTarget baseTarget, MediaType defaultProduces) {

		// @Path("PATH")

		target = baseTarget;

		Annotation pathAnnotation = method.getAnnotation(Path.class);
		// Expect @javax.ws.rs.Path(value=PATH)
		String path = WebClientHelper.parseValue(pathAnnotation);
		if (path != null) {
			target = baseTarget.path("/" + path);
		}

		// @Produces(MediaType.TYPE)

		Annotation producesAnnotation = method.getAnnotation(Produces.class);
		produces = WebClientHelper.parseMediaType(producesAnnotation, defaultProduces);

		returnType = method.getReturnType();

		Annotation[][] parameterAnnotations = method.getParameterAnnotations();

		templateIndexes = new HashMap<String, Integer>();
		templateValues = new HashMap<String, Object>();

		int index = 0;
		for (Annotation[] annotations : parameterAnnotations) {

			String paramName;

			// @QueryParam("NAME")

			paramName = WebClientHelper.parse(annotations, QueryParam.class);
			if (paramName != null) {

				String paramTemplateName = "_" + paramName + "_";
				target = target.queryParam(paramName, "{" + paramTemplateName + "}");

				templateIndexes.put(paramTemplateName, index);
			}

			// @PathParam("NAME")

			paramName = WebClientHelper.parse(annotations, PathParam.class);
			if (paramName != null) {

				templateIndexes.put(paramName, index);
			}

			index++;
		}
	}

	protected Invocation.Builder request(Object[] args) {

		if (!templateIndexes.isEmpty()) {

			// Resolve template values from the actual method arguments.

			for (Entry<String, Integer> entry : templateIndexes.entrySet()) {
				templateValues.put(entry.getKey(), args[entry.getValue()]);
			}

			target = target.resolveTemplates(templateValues);
		}

		return target.request(produces);
	}

	protected Class<?> getReturnType() {
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

class PutMethod extends WebMethod {

	private MediaType consumes;
	private int entityIndex;

	PutMethod(Method method, WebTarget baseTarget, MediaType defaultProduces, MediaType defaultConsumes) {
		super(method, baseTarget, defaultProduces);

		// @Consumes(MediaType.TYPE)

		Annotation consumesAnnotation = method.getAnnotation(Consumes.class);
		consumes = WebClientHelper.parseMediaType(consumesAnnotation, defaultConsumes);

		// WARNING: For a PUT method, it is assumed the last parameter contains the entity to put
		entityIndex = method.getParameterCount() - 1;
	}

	@Override
	public Object invoke(Object proxy, Object[] args) {
		return request(args).put(Entity.entity(args[entityIndex], consumes), getReturnType());
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
