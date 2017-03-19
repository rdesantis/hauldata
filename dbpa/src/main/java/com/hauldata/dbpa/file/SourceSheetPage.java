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

import java.io.IOException;

public class SourceSheetPage extends SourcePage {

	protected static class Factory implements SourcePage.Factory {

		private Book.Factory bookFactory;
		private Sheet.Factory sheetFactory;

		Factory(Book.Factory bookFactory, Sheet.Factory sheetFactory) {
			this.bookFactory = bookFactory;
			this.sheetFactory = sheetFactory;
		}

		@Override
		public SourcePage open(File.Owner fileOwner, PageIdentifier id, SourceHeaders headers) throws IOException {

			Book book = readBook(fileOwner, id);
			Sheet sheet = (Sheet)Sheet.getForOpen(book, ((SheetIdentifier)id).getSheetName(), sheetFactory);
			sheet.setHeaders(headers);
			sheet.open();
			sheet.setOpen(true);

			return new SourceSheetPage(book, sheet);
		}

		@Override
		public SourcePage load(File.Owner fileOwner, PageIdentifier id) throws IOException {

			Book book = readBook(fileOwner, id);
			Sheet sheet = (Sheet)Sheet.getForLoad(book, ((SheetIdentifier)id).getSheetName(), sheetFactory);
			sheet.load();

			return new SourceSheetPage(book, sheet);
		}

		@Override
		public SourcePage read(File.Owner fileOwner, PageIdentifier id, SourceHeaders headers) throws IOException {

			Book book = readBook(fileOwner, id);
			Sheet sheet = (Sheet)Sheet.getForRead(book, ((SheetIdentifier)id).getSheetName(), sheetFactory);
			if (!sheet.isOpen()) {
				sheet.setHeaders(headers);
				sheet.open();
				sheet.setOpen(true);
			}
			else {
				sheet.load();
			}

			return new SourceSheetPage(book, sheet);
		}

		private Book readBook(File.Owner fileOwner, PageIdentifier id) throws IOException {

			Book book = (Book)File.getForRead(fileOwner, id.getPath(), bookFactory);
			if (!book.isOpen()) {
				book.open();
				book.setOpen(true);
			}
			else {
				book.load();
			}
			return book;
		}
	}

	protected Book book;

	protected SourceSheetPage(Book book, Sheet sheet) {
		super(sheet);
		this.book = book;
	}
}
