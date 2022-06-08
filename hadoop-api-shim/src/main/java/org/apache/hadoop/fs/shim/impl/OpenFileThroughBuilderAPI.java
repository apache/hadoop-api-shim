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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.functional.FutureIO;

import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.OPENFILE;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.getMethod;

/**
 * Open a file through the builder API.
 * The only builder methods looked up and invoked are
 * opt(String, String), and must(String, String).
 */
public final class OpenFileThroughBuilderAPI
    extends AbstractAPIShim<FileSystem> implements ExecuteOpenFile {

  private static final Logger LOG = LoggerFactory.getLogger(OpenFileThroughBuilderAPI.class);

  private final Method openFileMethod;

  private final AtomicInteger openFileFailures = new AtomicInteger();

  /**
   * Last exception swallowed in openFile();
   */
  private volatile Exception lastOpenFileException;

  /**
   * Constructor.
   *
   * @param instance FS instance to shim.
   */
  public OpenFileThroughBuilderAPI(
      final FileSystem instance) {
    super(FileSystem.class, instance);
    this.openFileMethod = getMethod(instance.getClass(), "openFile", Path.class);
  }

  /**
   * Record an exception during openFile.
   * Increments the counter and sets the
   * {@link #lastOpenFileException} to the value
   *
   * @param ex caught exception.
   */
  private void openFileException(Path path, Exception ex) {
    LOG.info("Failed to use openFile({})", path, ex);
    openFileFailures.incrementAndGet();
    if (ex != null) {
      lastOpenFileException = ex;
    }

  }

  @Override
  public boolean isImplemented(final String capability) {
    return OPENFILE.equalsIgnoreCase(capability)
        ? openFileFound()
        : false;
  }

  public boolean openFileFound() {
    return openFileMethod != null;
  }

  /**
   * Count of openfile failures.
   *
   * @return counter
   */
  public int getOpenFileFailures() {
    return openFileFailures.get();
  }

  /**
   * Use the openFile method to open a file from the builder API.
   *
   * @param source builder to read.
   *
   * @return the input stream
   *
   * @throws ClassCastException classcast problems
   * @throws UnsupportedOperationException if the reflection calls failed.
   */
  @SuppressWarnings("unchecked")
  @Override
  public CompletableFuture<FSDataInputStream> executeOpenFile(final OpenFileBuilder source)
      throws IllegalArgumentException, UnsupportedOperationException, IOException {

    FileSystem fs = getInstance();
    Path path = fs.makeQualified(source.getPath());
    LOG.debug("Opening file at {} through builder API", path);

    try {
      Object builder = openFileMethod.invoke(fs, path);
      Class<?> builderClass = builder.getClass();
      // optional paraketers
      Method opt = builderClass.getMethod("opt", String.class, String.class);
      Configuration options = source.getOptions();
      for (Map.Entry<String, String> option : options) {
        opt.invoke(builder, option.getKey(), option.getValue());
      }
      // mandatory parameters
      Method must = builderClass.getMethod("must", String.class, String.class);
      for (String k : source.getMandatoryKeys()) {
        must.invoke(builder, k, options.get(k));
      }
      Method build = builderClass.getMethod("build");
      build.setAccessible(true);
      Object result = build.invoke(builder);
      // cast and return. may raise ClassCastException which will
      // be thrown.
      CompletableFuture<FSDataInputStream> future = (CompletableFuture<FSDataInputStream>) result;

      return future;

    } catch (ClassCastException e) {
      // this a bug in the code, rethrow
      openFileException(path, e);
      throw e;
    } catch (InvocationTargetException e) {
      // an exception was reaised by the method, so examine it
      // this is not something to consider an API failure
      final IOException ioe = FutureIO.unwrapInnerException(e);
      LOG.debug("Openfile failure for {}", path, ioe);
      throw ioe;

    } catch (IllegalAccessException | NoSuchMethodException e) {
      // downgrade on all failures, even classic IOEs...let the fallback handle them.
      openFileException(path, e);
      throw new UnsupportedOperationException(e);
    }

  }

}
