// Copyright (c) 2017 Aalto University
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

package org.seqdoop.hadoop_bam;

import com.intel.gkl.compression.IntelDeflaterFactory;
import com.intel.gkl.compression.IntelInflaterFactory;
import htsjdk.samtools.util.zip.DeflaterFactory;
import htsjdk.samtools.util.zip.InflaterFactory;

/**
 * GKL code is kept here so this class is only loaded when the Intel inflator or
 * deflator is used. This means that users that aren't using these features don't
 * have to put the GKL JAR on their classpath (since it's a provided dependency).
 */
class IntelGKLAccessor {
  static InflaterFactory newInflatorFactor() {
    return new IntelInflaterFactory();
  }
  static DeflaterFactory newDeflaterFactory() {
    return new IntelDeflaterFactory();
  }
}
