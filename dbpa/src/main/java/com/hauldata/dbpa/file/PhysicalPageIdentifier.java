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

package com.hauldata.dbpa.file;

import java.io.IOException;
import java.nio.file.Path;

public abstract class PhysicalPageIdentifier implements PageIdentifier {

	protected FileHandler handler;
	protected Path path;

	protected PhysicalPageIdentifier(FileHandler handler, Path path) {
		this.handler = handler;
		this.path = path;
	}

	public Path getPath() {
		return path;
	}

	public TargetPage create(File.Owner fileOwner, PageOptions options, TargetHeaders headers) throws IOException {
		return handler.getTargetFactory().create(fileOwner, this, options, headers);
	}

	@Override
	public TargetPage append(File.Owner fileOwner) throws IOException {
		return handler.getTargetFactory().append(fileOwner, this);
	}

	@Override
	public TargetPage write(File.Owner fileOwner, PageOptions options, TargetHeaders headers) throws IOException {
		return handler.getTargetFactory().write(fileOwner, this, options, headers);
	}

	@Override
	public SourcePage open(File.Owner fileOwner, SourceHeaders headers) throws IOException {
		return handler.getSourceFactory().open(fileOwner, this, headers);
	}

	@Override
	public SourcePage load(File.Owner fileOwner) throws IOException {
		return handler.getSourceFactory().load(fileOwner, this);
	}

	@Override
	public SourcePage read(File.Owner fileOwner, SourceHeaders headers) throws IOException {
		return handler.getSourceFactory().read(fileOwner, this, headers);
	}
}
