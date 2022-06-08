/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.shim.api;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shim.impl.FSDataInputStreamShimImpl;
import org.apache.hadoop.fs.shim.impl.FileSystemShimImpl;

/**
 * Binding class to create shim instances.
 */
public final class ShimFactory {

  /**
   * Shim FS APIs.
   * @param fs
   * @return the shim
   */
  public static FileSystemShim shimFileSystem(FileSystem filesystem) {
    return new FileSystemShimImpl(filesystem);
  }

  /**
   * Shim an FSDataInputStream instance.
   *
   * @param in the stream
   *
   * @return the shim
   */
  public static FSDataInputStreamShim shimFSDataInputStream(
      FSDataInputStream in) {
    return new FSDataInputStreamShimImpl(in);
  }

}
