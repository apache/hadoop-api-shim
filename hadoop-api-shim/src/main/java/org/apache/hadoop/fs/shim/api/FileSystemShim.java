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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.shim.functional.FutureDataInputStreamBuilder;

/**
 * Shim for the Hadoop {@code FileSystem} class.
 * Some of this is fairly complex, especially when fallback methods are provided...
 * separate shims are used to help here.
 */
public interface FileSystemShim extends APIShim<FileSystem> {
  /**
   * The openfile call.
   *
   * @param path
   *
   * @return
   *
   * @throws IOException
   * @throws UnsupportedOperationException
   */
  FutureDataInputStreamBuilder openFile(Path path)
      throws IOException, UnsupportedOperationException;

  /**
   * Is the openFile method available?
   *
   * @return true if the FS implements the method.
   */
  boolean openFileFound();

  /**
   * Is the path capabilities API available?
   * @return true if the API is found on the underlying filesystem.
   */
  boolean pathCapabilitiesFound();

  /**
   * Has path capability. HADOOP-15691/3.2.2+
   *
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   *
   * @return true if the capability is supported under that part of the FS.
   *
   * @throws IOException this should not be raised, except on problems
   * resolving paths or relaying the call.
   * @throws IllegalArgumentException invalid arguments
   */
  boolean hasPathCapability(Path path, String capability)
      throws IOException;

  /**
   * Is msync available?
   *
   * @return true if the mmsync method is available.
   */
  boolean msyncFound();

  /**
   * Synchronize client metadata state; relevant for
   * HDFS where it may be a slow operation.
   * <p>
   * In many versions of hadoop, but not cloudera CDH7.
   * A no-op if not implemented.
   *
   * @throws IOException If an I/O error occurred.
   */
  void msync() throws IOException;
}
