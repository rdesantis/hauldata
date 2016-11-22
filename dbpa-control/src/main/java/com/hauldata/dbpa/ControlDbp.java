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

package com.hauldata.dbpa;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
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

import com.hauldata.dbpa.control.resources.SchedulesResourceClient;
import com.hauldata.dbpa.manage.api.ScheduleValidation;
import com.hauldata.dbpa.manage.resources.SchedulesResourceInterface;

public class ControlDbp {

	enum KW {

		// Commands

		PUT,
		GET,
		DELETE,
		LIST,
		VALIDATE,
		SHOW,
		ALTER,
		RUN,
		STOP,
		START,
		CONFIRM,

		// Objects

		SCRIPT,
		PROPERTIES,
		SCHEDULE,
		JOB,
		//RUN,		// Also a command name
		RUNNING,
		MANAGER,
		SERVICE,
		SCHEMA,

		// Attributes

		ARGUMENTS,
		ENABLED,

		// Other

		FROM,
		TO,
		ON,
		OFF,
		NO
	}

	public static void main(String[] args) throws ReflectiveOperationException {

		String baseUrl = "http://localhost:8080";
		SchedulesResourceInterface client = new SchedulesResourceClient(baseUrl);

		SchedulesResourceInterface webClient = (SchedulesResourceInterface)WebClient.of(SchedulesResourceInterface.class, baseUrl);

		// get

		String name = "NotGarbage";
		String schedule = "[get failed]";

		System.out.println();

		try {
			schedule = webClient.get(name);
//			schedule = client.get(name);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + name + "' = " + schedule);

		// getNames

		String likeName = "garbage%";
		List<String> names = null;

		System.out.println();

		try {
			names = webClient.getNames(likeName);
//			names = client.getNames(likeName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of schedule names:");
		if (names == null) {
			System.out.println("[empty]");
		}
		else {
			System.out.println(names.toString());
		}

		// validate

		String validateName = "invalid";
		ScheduleValidation validation = null;

		System.out.println();

		try {
			validation = webClient.validate(validateName);
//			validation = client.validate(validateName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + validateName + "' validation:");
		if (validation == null) {
			System.out.println("[empty]");
		}
		else {
			System.out.println("{" + String.valueOf(validation.isValid()) + ", \"" + validation.getValidationMessage() + "\"}");
		}

		// put

		String putName = "Weekly Friday";
		String putSchedule = "WEEKLY FRIDAY";

		System.out.println();

		int id = -1;
		try {
			id = webClient.put(putName, putSchedule);
//			id = client.put(putName, putSchedule);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + putName + "' id: " + String.valueOf(id));

		// delete

		String deletedName = "Doesn't exist";
		boolean deleted = false;

		System.out.println();

		try {
			webClient.delete(deletedName);
//			client.delete(deletedName);
			deleted = true;
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + deletedName + "' deleted? " + String.valueOf(deleted));

		// get doesn't exist

		name = deletedName;
		schedule = null;

		System.out.println();

		try {
			schedule = webClient.get(name);
//			schedule = client.get(name);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + name + "' = " + schedule);
	}
}

class WebClient {

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
	 * Parse the regex group from the annotation, which may be null
	 * @param annotation is the annotation to parse
	 * @param patternWithGroup is the pattern including the group to return
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
	 * Parse the "value =" value from the annotation, which may be null
	 * @param annotation is the annotation to parse
	 * @return the value parsed or null if the annotation is null or it does not have value
	 */
	public static String parseValue(Annotation annotation) {
		return parse(annotation, "\\(value=(.+)\\)");
	}

	/**
	 * Parse media type from the annotation, which may be null
	 * @param annotation is the annotation to parse
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

class WebClientInvocationHandler implements InvocationHandler {

	private Map<Method, WebMethod> webMethods;

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

		Annotation comsumesAnnotation = clientInterface.getAnnotation(Consumes.class);
		MediaType defaultConsumes = WebClientHelper.parseMediaType(comsumesAnnotation, null);

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

abstract class WebMethod {

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

		Annotation producesAnnotation = method.getAnnotation(Consumes.class);
		consumes = WebClientHelper.parseMediaType(producesAnnotation, defaultConsumes);

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
