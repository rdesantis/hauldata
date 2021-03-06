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

package com.hauldata.dbpa.file;

import java.util.ArrayList;

public class SourceHeaders extends Headers {

	protected boolean mustValidate;
	protected boolean toMetadata;

	public SourceHeaders() {
		this(false, false, false, new ArrayList<String>());
	}

	public SourceHeaders(
			boolean exist,
			boolean mustValidate,
			boolean toMetadata,
			ArrayList<String> captions) {

		super(exist, captions);

		this.mustValidate = mustValidate;
		this.toMetadata = toMetadata;
	}

	public boolean mustValidate() {
		return mustValidate;
	}

	public boolean toMetadata() {
		return toMetadata;
	}
}
