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

package com.hauldata.dbpa.file.html;

import java.io.IOException;

import com.hauldata.dbpa.file.File;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.SourcePage;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.dbpa.file.TargetPage;
import com.hauldata.dbpa.file.File.Owner;
import com.hauldata.dbpa.variable.Variable;

public class HtmlPageIdentifier implements PageIdentifier {

	private Variable<String> variable;

	public HtmlPageIdentifier(Variable<String> variable) {
		this.variable = variable;
	}

	@Override
	public String getName() {
		return variable.getName();
	}

	@Override
	public TargetPage write(Owner fileOwner, PageOptions options, TargetHeaders headers) throws IOException {

		Html html = new Html(variable, (HtmlOptions)options);
		html.setHeaders(headers);

		if (headers.exist() && !headers.fromMetadata()) {
			for (int columnIndex = 1; columnIndex <= headers.getColumnCount(); ++columnIndex) {
				html.writeColumn(columnIndex, headers.getCaption(columnIndex - 1));
			}
		}

		return new HtmlTargetPage(html);
	}

	// Never called.
	@Override public TargetPage create(Owner fileOwner, PageOptions options, TargetHeaders headers) { return null; }
	@Override public TargetPage append(Owner fileOwner) { return null; }
	@Override public SourcePage open(Owner fileOwner, SourceHeaders headers) { return null; }
	@Override public SourcePage load(Owner fileOwner) { return null; }
	@Override public SourcePage read(Owner fileOwner, SourceHeaders headers) { return null; }
}
