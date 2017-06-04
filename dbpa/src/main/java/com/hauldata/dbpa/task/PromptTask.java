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

package com.hauldata.dbpa.task;

import java.io.Console;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableBase;
import com.hauldata.dbpa.variable.VariablesFromStrings;
import com.hauldata.util.tokenizer.KeywordValueTokenizer;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Token;

public class PromptTask extends Task {

	private Expression<String> prompt;
	private boolean isPassword;
	private List<VariableBase> variables;

	public PromptTask(
			Prologue prologue,
			Expression<String> prompt,
			boolean isPassword,
			List<VariableBase> variables) {
		super(prologue);
		this.prompt = prompt;
		this.isPassword = isPassword;
		this.variables = variables;
	}

	@Override
	protected void execute(Context context) throws Exception {

		String evaluatedPrompt = prompt.evaluate();
		if (evaluatedPrompt == null) {
			throw new RuntimeException("Prompt expression evaluates to NULL");
		}

		Console console = System.console();
		if (console == null) {
			throw new RuntimeException("No console is available to read from");
		}

		String input;
		if (isPassword) {
			char[] passCharacters = console.readPassword(evaluatedPrompt);
			input = new String(passCharacters);
		}
		else {
			input = console.readLine(evaluatedPrompt);
		}

		List<String> args = new LinkedList<String>();
		if (variables.size() == 1) {
			args.add(input);
		}
		else {
			KeywordValueTokenizer tokenizer = new KeywordValueTokenizer(new StringReader(input));
			while (tokenizer.hasNext()) {
				Token token = tokenizer.nextToken();
				String arg = token instanceof Quoted ? ((Quoted)token).getBody() : token.getImage();
				args.add(arg);
			}
			tokenizer.close();
		}

		VariablesFromStrings.set(variables, args.stream().toArray(String[]::new));
	}
}
