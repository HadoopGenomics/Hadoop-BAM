// Copyright (c) 2011 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// File created: 2011-06-14 13:38:57

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.io.PrintStream;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;

public class View extends CLIPlugin {
	private static final NavigableMap<String, String> paramDescs =
		new TreeMap<String, String>();

	public View() { super("view", "BAM viewing", "1.0", "", paramDescs); }
	static {
		paramDescs.put("-H", "print header only");
	}

	@Override protected int run(CmdLineParser parser) {
		return 0;
	}
}
