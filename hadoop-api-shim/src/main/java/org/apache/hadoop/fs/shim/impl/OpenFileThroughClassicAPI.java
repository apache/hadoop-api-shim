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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.shim.functional.FutureIO.eval;

/**
 * Build through the classic API.
 * The opening is asynchronous.
 */
public class OpenFileThroughClassicAPI implements ExecuteOpenFile {
  private static final Logger LOG = LoggerFactory.getLogger(OpenFileThroughClassicAPI.class);

  private final FileSystem fileSystem;

  public OpenFileThroughClassicAPI(FileSystem fs) {
    fileSystem = fs;
  }

  @Override
  public CompletableFuture<FSDataInputStream> executeOpenFile(
      final OpenFileBuilder source)
      throws IllegalArgumentException, UnsupportedOperationException, IOException {

    Path path = fileSystem.makeQualified(source.getPath());
    LOG.debug("Opening file at {} through builder API", path);

    if (!source.getMandatoryKeys().isEmpty()) {
      throw new IllegalArgumentException("Mandatory keys not supported");
    }
    return eval(() ->
        fileSystem.open(source.getPath()));
  }
}
