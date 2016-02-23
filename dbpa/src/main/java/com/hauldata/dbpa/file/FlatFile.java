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

package com.hauldata.dbpa.file;

import java.nio.file.Path;

public abstract class FlatFile extends File implements PageNode {

	protected Headers headers;

	public FlatFile(Owner owner, Path path) {
		super(owner, path);
		this.headers = null;
	}

	@Override
	public void setHeaders(Headers headers) {
		this.headers = headers;
	}

	@Override
	public ReadHeaders getReadHeaders() {
		return (ReadHeaders)headers;
	}

	@Override
	public WriteHeaders getWriteHeaders() {
		return (WriteHeaders)headers;
	}
}
