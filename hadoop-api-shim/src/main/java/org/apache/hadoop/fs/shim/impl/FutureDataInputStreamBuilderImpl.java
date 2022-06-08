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

package org.apache.hadoop.fs.shim.impl;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.functional.FutureDataInputStreamBuilder;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;

/**
 * Builder for input streams and subclasses whose return value is
 * actually a completable future: this allows for better asynchronous
 * operation.
 *
 * To be more generic, {@link #opt(String, int)} and {@link #must(String, int)}
 * variants provide implementation-agnostic way to customize the builder.
 * Each FS-specific builder implementation can interpret the FS-specific
 * options accordingly, for example:
 *
 * If the option is not related to the file system, the option will be ignored.
 * If the option is must, but not supported/known by the file system, an
 * {@link IllegalArgumentException} will be thrown.
 *
 */
public abstract class FutureDataInputStreamBuilderImpl
    extends AbstractFSBuilderImpl<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder>
    implements FutureDataInputStreamBuilder {

  private int bufferSize = IO_FILE_BUFFER_SIZE_DEFAULT;

  /**
   * Constructor.
   *
   * @param path path
   */
  protected FutureDataInputStreamBuilderImpl(@Nonnull Path path) {
    super(requireNonNull(path, "path"));
  }


  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Set the size of the buffer to be used.
   *
   * @param bufSize buffer size.
   * @return FutureDataInputStreamBuilder.
   */
  public FutureDataInputStreamBuilder bufferSize(int bufSize) {
    bufferSize = bufSize;
    return getThisBuilder();
  }

  /**
   * Get the builder.
   * This must be used after the constructor has been invoked to create
   * the actual builder: it allows for subclasses to do things after
   * construction.
   *
   * @return FutureDataInputStreamBuilder.
   */
  public FutureDataInputStreamBuilder builder() {
    return getThisBuilder();
  }

  @Override
  public FutureDataInputStreamBuilder getThisBuilder() {
    return this;
  }

}
