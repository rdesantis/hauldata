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

	public static abstract class AvailabilityException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		protected AvailabilityException(String message) { super(message); }
	}

	public static abstract class ConflictException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		protected ConflictException(String message) { super(message); }
	}

	public static abstract class SchemaException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		protected SchemaException(String message) { super(message); }
	}

	// Concrete classes

	public static class JobManagerNotAvailableException extends AvailabilityException {
		private static final long serialVersionUID = 1L;
		public JobManagerNotAvailableException() { super(notAvailableMessage); }
	}

	public static class JobManagerAlreadyUnavailableException extends ConflictException {
		private static final long serialVersionUID = 1L;
		public JobManagerAlreadyUnavailableException() { super(alreadyUnavailableMessage); }
	}

	public static class SchemaPropertiesNotFoundException extends SchemaException {
		private static final long serialVersionUID = 1L;
		public SchemaPropertiesNotFoundException() { super(schemaPropertiesNotFoundMessage); }
	}

	public static class SchemaTablePrefixPropertyNotFoundException extends SchemaException {
		private static final long serialVersionUID = 1L;
		public SchemaTablePrefixPropertyNotFoundException() { super(schemaTablePrefixPropertyNotFoundMessage); }
	}

	public static class JobSchedulerNotAvailableException extends AvailabilityException {
		private static final long serialVersionUID = 1L;
		public JobSchedulerNotAvailableException() { super(schedulerNotAvailableMessage); }
	}

	public static class JobManagerAlreadyStartedException extends ConflictException {
		private static final long serialVersionUID = 1L;
		public JobManagerAlreadyStartedException() { super(alreadyStartedMessage); }
	}

	public static class JobManagerNotStartedCantRunJobException extends ConflictException {
		private static final long serialVersionUID = 1L;
		public JobManagerNotStartedCantRunJobException() { super(mustStartupBeforeJobRunMessage); }
	}

	public static class JobManagerNotStartedNoJobRunningException extends ConflictException {
		private static final long serialVersionUID = 1L;
		public JobManagerNotStartedNoJobRunningException() { super(notStartedNoJobRunningMessage); }
	}

	public static class JobManagerNotStartedException extends ConflictException {
		private static final long serialVersionUID = 1L;
		public JobManagerNotStartedException() { super(notStartedMessage); }
	}
}
