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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;

import com.hauldata.util.tokenizer.BacktrackingTokenizer;

public abstract class TextFile extends FlatFile {

	private static final String preferredCharset = "windows-1252";
	private static final String fallbackCharset = "ISO-8859-1";

	public TextFile(Owner owner, Path path, FileOptions options) {
		super(owner, path, options);
	}

	public TextFile(Owner owner, Path path) {
		this(owner, path, null);
	}

	public static Charset getDefaultCharset() {
		return defaultCharset;
	}

	private static Charset defaultCharset = Charset.forName(Charset.isSupported(preferredCharset) ? preferredCharset : fallbackCharset);

	private static class Format {

		public Charset charset;
		public boolean hasBom;

		public static Format defaultFormat = new Format(defaultCharset, false);

		Format(
				Charset charset,
				boolean hasBom) {
			this.charset = charset;
			this.hasBom = hasBom;
		}
	}

	/**
	 * @return the actual character set of the file if it exists and can be determined;
	 * otherwise the default character set.
	 */
	private Format getFormat() {

		Charset charset = null;
		boolean hasBom = false;

		// Check if the file has a BOM; see https://en.wikipedia.org/wiki/Byte_order_mark

		FileInputStream in = null;
		try {
			in = new FileInputStream(getName());
			byte bom[] = new byte[3];
			int count = in.read(bom, 0, 3);

			if (
					(3 <= count) &&
					(bom[0] == (byte)0xef && bom[1] == (byte)0xbb && bom[2] == (byte)0xbf)) {
				charset = Charset.forName("UTF-8");
				hasBom = true;
			}
			else if (
					(2 <= count)  &&
					(bom[0] == (byte)0xfe && bom[1] == (byte)0xff)) {
				charset = Charset.forName("UTF-16BE");
				hasBom = true;
			}
			else if (
					(2 <= count)  &&
					(bom[0] == (byte)0xff && bom[1] == (byte)0xfe)) {
				charset = Charset.forName("UTF-16LE");
				hasBom = true;
			}
		}
		catch (IOException /* catches FileNotFoundException */ ex) {
			// File doesn't exist or can't be read.
		}
		finally {
			if (in != null) {
				try { in.close(); } catch (IOException ex) {}
			}
		}

		return (charset != null) ? new Format(charset, hasBom) : Format.defaultFormat;
	}

	protected BufferedReader getReader() throws IOException {

		Format format = getFormat();

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(getName()), format.charset));

		if (format.hasBom) {
			reader.read();
		}

		return reader;
	}

	protected BufferedWriter getWriter(boolean append) throws IOException {

		Format format = append ? getFormat() : Format.defaultFormat;

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(getName(), append), format.charset));

		return writer;
	}

	protected static class TargetOptions implements FileOptions {

		public static final TargetOptions DEFAULT = new TargetOptions();

		private String endOfLine = String.format("%n");

		public String getEndOfLine() {
			return endOfLine;
		}

		public static class Parser implements FileOptions.Parser {

			@Override
			public FileOptions parse(BacktrackingTokenizer tokenizer) throws IOException {

				TargetOptions options = new TargetOptions();
				boolean mayBeMoreOptions;
				do {
					mayBeMoreOptions = parse(tokenizer, options);
				}
				while (mayBeMoreOptions);
				return options;
			}

			protected boolean parse(BacktrackingTokenizer tokenizer, TargetOptions options) throws IOException {

				boolean mayBeMoreOptions = true;
				if ((mayBeMoreOptions = tokenizer.skipWordIgnoreCase("CRLF"))) {
					options.endOfLine = "\r\n";
				}
				else if ((mayBeMoreOptions = tokenizer.skipWordIgnoreCase("LF"))) {
					options.endOfLine = "\n";
				}
				return mayBeMoreOptions;
			}
		}
	}

	protected TargetOptions getTargetOptions() {
		// TODO: This function should just be the (TargetOptions)getOptions() cast.
		// But due to undocumented APPEND without prior CREATE, getOptions() may return null.
		return getOptions() != null ? (TargetOptions)getOptions() : TargetOptions.DEFAULT;
	}
}
