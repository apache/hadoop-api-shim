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
package org.apache.hadoop.fs.shim.functional;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shim.api.FSBuilder;

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
 * source {@code org.apache.hadoop.fs.FutureDataInputStreamBuilder}
 *
 */

public interface FutureDataInputStreamBuilder
    extends FSBuilder<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder> {

  @Override
  CompletableFuture<FSDataInputStream> build()
      throws IllegalArgumentException, UnsupportedOperationException,
      IOException;

}
