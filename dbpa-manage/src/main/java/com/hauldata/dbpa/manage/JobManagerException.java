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

package com.hauldata.dbpa.manage;

public class JobManagerException {

	public static final String notAvailableMessage = "Job Manager is not available";
	public static final String alreadyUnavailableMessage = "Job Manager is already unavailable";
	public static final String schemaPropertiesNotFoundMessage = "Job schema properties not found";
	public static final String schemaTablePrefixPropertyNotFoundMessage = "Job schema tablePrefix property not found";
	public static final String schedulerNotAvailableMessage = "Job Scheduler is not available";
	public static final String alreadyStartedMessage = "Job Manager is already started";
	public static final String mustStartupBeforeJobRunMessage = "Must startup Job Manager before running jobs";
	public static final String notStartedNoJobRunningMessage = "Job Manager is not started; no jobs are running";
	public static final String notStartedMessage = "Job Manager is not started";

	// Abstract classes

	public enum Type { AVAILABILITY, CONFLICT, SCHEMA };

	public static abstract class BaseException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		private Type type;
		protected BaseException(Type type, String message) { super(message); this.type = type; }
		public Type getType() { return type; }
	}

	public static abstract class AvailabilityException extends BaseException {
		private static final long serialVersionUID = 1L;
		protected AvailabilityException(String message) { super(Type.AVAILABILITY, message); }
	}

	public static abstract class ConflictException extends BaseException {
		private static final long serialVersionUID = 1L;
		protected ConflictException(String message) { super(Type.CONFLICT, message); }
	}

	public static abstract class SchemaException extends BaseException {
		private static final long serialVersionUID = 1L;
		protected SchemaException(String message) { super(Type.SCHEMA, message); }
	}

	// Concrete classes

	public static class NotAvailable extends AvailabilityException {
		private static final long serialVersionUID = 1L;
		public NotAvailable() { super(notAvailableMessage); }
	}

	public static class AlreadyUnavailable extends ConflictException {
		private static final long serialVersionUID = 1L;
		public AlreadyUnavailable() { super(alreadyUnavailableMessage); }
	}

	public static class SchemaPropertiesNotFound extends SchemaException {
		private static final long serialVersionUID = 1L;
		public SchemaPropertiesNotFound() { super(schemaPropertiesNotFoundMessage); }
	}

	public static class SchemaTablePrefixPropertyNotFound extends SchemaException {
		private static final long serialVersionUID = 1L;
		public SchemaTablePrefixPropertyNotFound() { super(schemaTablePrefixPropertyNotFoundMessage); }
	}

	public static class SchedulerNotAvailable extends AvailabilityException {
		private static final long serialVersionUID = 1L;
		public SchedulerNotAvailable() { super(schedulerNotAvailableMessage); }
	}

	public static class AlreadyStarted extends ConflictException {
		private static final long serialVersionUID = 1L;
		public AlreadyStarted() { super(alreadyStartedMessage); }
	}

	public static class NotStarted extends ConflictException {
		private static final long serialVersionUID = 1L;
		public NotStarted() { super(notStartedMessage); }
		public NotStarted(String message) { super(message); }
	}
}
