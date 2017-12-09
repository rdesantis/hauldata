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

package com.hauldata.dbpa.file.book;

import com.hauldata.dbpa.file.Headers;
import com.hauldata.dbpa.file.Node;
import com.hauldata.dbpa.file.PageNode;
import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.TargetHeaders;

public abstract class Sheet extends Node implements PageNode {

	private PageOptions options;

	protected Headers headers;

	public Sheet(Owner owner, String name, PageOptions options) {
		super(owner, name);
		this.options = options;
		this.headers = null;
	}

	public Sheet(Owner owner, String name) {
		this(owner, name, null);
	}

	public PageOptions getOptions() {
		return options;
	}

	@Override
	public void setHeaders(Headers headers) {
		this.headers = headers;
	}

	@Override
	public SourceHeaders getSourceHeaders() {
		return (SourceHeaders)headers;
	}

	@Override
	public TargetHeaders getTargetHeaders() {
		return (TargetHeaders)headers;
	}

	@Override
	public String getName() {
		return (String)key;
	}
}
