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

import java.nio.charset.Charset;
import java.nio.file.Path;

public abstract class TextFile extends FlatFile {

	private static final String preferredCharset = "windows-1252";
	private static final String fallbackCharset = "ISO-8859-1";
	
	public TextFile(Owner owner, Path path) {
		super(owner, path);
	}

	public static Charset getCharset() {
		return Charset.forName(Charset.isSupported(preferredCharset) ? preferredCharset : fallbackCharset);
	}
}
