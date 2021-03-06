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

package com.hauldata.dbpa.file.flat;

import java.io.IOException;

import com.hauldata.dbpa.file.File;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.PhysicalPageIdentifier;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.dbpa.file.TargetPage;

public class TargetFilePage extends TargetPage {

	public static class Factory implements TargetPage.Factory {

		private File.Factory fileFactory;

		public Factory(File.Factory fileFactory) {
			this.fileFactory = fileFactory;
		}

		@Override
		public TargetPage create(File.Owner fileOwner, PageIdentifier id, PageOptions options, TargetHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForCreate(fileOwner, ((PhysicalPageIdentifier)id).getPath(), fileFactory, options);
			file.setHeaders(headers);
			file.create();
			file.setOpen(true);
			return new TargetFilePage(file);
		}

		@Override
		public TargetPage append(File.Owner fileOwner, PageIdentifier id) throws IOException {
			FlatFile file = (FlatFile)File.getForAppend(fileOwner, ((PhysicalPageIdentifier)id).getPath(), fileFactory);
			if (!file.isOpen()) {
				file.setHeaders(new TargetHeaders());
				file.append();
				file.setOpen(true);
			}
			return new TargetFilePage(file);
		}

		@Override
		public TargetPage write(File.Owner fileOwner, PageIdentifier id, PageOptions options, TargetHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForWrite(fileOwner, ((PhysicalPageIdentifier)id).getPath(), fileFactory, options);
			if (!file.isOpen()) {
				file.setHeaders(headers);
				file.create();
				file.setOpen(true);
			}
			return new TargetFilePage(file);
		}
	}

	protected TargetFilePage(FlatFile file) {
		super(file);
	}
}
