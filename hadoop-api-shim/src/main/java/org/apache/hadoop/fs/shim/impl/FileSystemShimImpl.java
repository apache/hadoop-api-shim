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
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.api.FileSystemShim;
import org.apache.hadoop.fs.shim.functional.FutureDataInputStreamBuilder;

import static org.apache.hadoop.fs.shim.api.ShimConstants.FS_OPTION_SHIM_OPENFILE_ENABLED;
import static org.apache.hadoop.fs.shim.api.ShimConstants.FS_OPTION_SHIM_OPENFILE_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.MSYNC;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.OPENFILE;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.PATH_CAPABILITIES;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.availability;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.loadInvocation;

/**
 * Shim for the Hadoop {@code FileSystem} class.
 * Some of this is fairly complex, especially when fallback methods are provided...
 * separate shims are used to help here.
 */
public class FileSystemShimImpl extends AbstractAPIShim<FileSystem>
    implements FileSystemShim {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemShimImpl.class);

  private final Invocation hasPathCapabilityMethod;
  private final Invocation msyncMethod;

  private final ExecuteOpenFile classicOpenFile;

  /**
   * Builder API; may be null
   */
  private final OpenFileThroughBuilderAPI openFileThroughBuilder;

  /**
   * Should the openFile() API be used? default is false.
   */
  private final AtomicBoolean useOpenFileAPI = new AtomicBoolean(false);

  /**
   * File Opener.
   */
  private final OpenFileThroughAvailableOperation executeOpenFile;

  /**
   * Constructor
   *
   * @param instance instance to bind to.
   */
  public FileSystemShimImpl(final FileSystem instance) {
    this(instance, false);
  }

  /**
   * Costructor, primarily for testing.
   *
   * @param instance instance to bind to.
   * @param raiseExceptionsOnOpenFileFailures raise exceptions rather than downgrade on openFile builder failures.
   */
  public FileSystemShimImpl(
      final FileSystem instance,
      final boolean raiseExceptionsOnOpenFileFailures) {
    super(FileSystem.class, instance);

    // this is always present and used as the fallback
    classicOpenFile = new OpenFileThroughClassicAPI(getInstance());

    // use the builder if present, and configured.
    OpenFileThroughBuilderAPI builderAPI = null;
    Configuration conf = instance.getConf();
    if (conf.getBoolean(FS_OPTION_SHIM_OPENFILE_ENABLED,
        FS_OPTION_SHIM_OPENFILE_ENABLED_DEFAULT)) {
      builderAPI = new OpenFileThroughBuilderAPI(getInstance());
      if (builderAPI.openFileFound()) {
        //the method is present, so bind to it.
        openFileThroughBuilder = builderAPI;
        useOpenFileAPI.set(true);
      } else {
        LOG.debug("Builder API enabled but not found");
        openFileThroughBuilder = null;
      }
    } else {
      // builder not enabled
      LOG.debug("Builder API not enabled");
      openFileThroughBuilder = null;
    }

    // the simpler methods.
    Class<FileSystem> clazz = getClazz();

    hasPathCapabilityMethod = loadInvocation(clazz, "hasPathCapability",
        Boolean.class,
        Path.class, String.class);

    msyncMethod = loadInvocation(clazz, "msync", Void.class);

    executeOpenFile = new OpenFileThroughAvailableOperation();
  }

  private void setLongOpt(Object builder, Method opt, String key, long len)
      throws InvocationTargetException, IllegalAccessException {
    if (len >= 0) {
      opt.invoke(builder, key, Long.toString(len));
    }
  }

  @Override
  public FutureDataInputStreamBuilder openFile(Path path)
      throws IOException, UnsupportedOperationException {
    return new OpenFileBuilder(executeOpenFile, path);
  }

  /**
   * Static class to handle the openFile operation.
   * Why not just make the shim claas implement the method?
   * Exposes too much of the implementation.
   */
  private final class OpenFileThroughAvailableOperation implements ExecuteOpenFile {
    @Override
    public CompletableFuture<FSDataInputStream> executeOpenFile(final OpenFileBuilder builder)
        throws IllegalArgumentException, UnsupportedOperationException, IOException {

      if (openFileFound()) {
        try {
          // use the builder API; return the result
          return openFileThroughBuilder.executeOpenFile(builder);
        } catch (ClassCastException | UnsupportedOperationException e) {
          LOG.warn(
              "Failed to open file using openFileThroughBuilder, falling back to classicOpenFile",
              e);
          // disable the API for this shim instance and fall back to classicOpenFile.
          useOpenFileAPI.set(false);
        }
      }
      return classicOpenFile.executeOpenFile(builder);
    }

    @Override
    public boolean isImplemented(final String capability) {
      return OPENFILE.equalsIgnoreCase(capability)
          ? openFileFound()
          : false;
    }
  }

  @Override
  public boolean openFileFound() {
    return useOpenFileAPI.get();
  }

  /**
   * Count of openfile failures.
   *
   * @return counter
   */
  public int getOpenFileFailures() {
    return openFileThroughBuilder.getOpenFileFailures();
  }

  @Override
  public boolean pathCapabilitiesFound() {
    return hasPathCapabilityMethod.available();
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    if (!pathCapabilitiesFound()) {
      return false;
    }
    try {
      return (Boolean) hasPathCapabilityMethod.invoke(getInstance(), path, capability);
    } catch (ClassCastException | IllegalArgumentException e) {
      LOG.debug("Failure of hasPathCapability({}, {})", path, capability, e);
      return false;
    }
  }

  @Override
  public boolean msyncFound() {
    return msyncMethod.available();
  }

  @Override
  public void msync() throws IOException {
    if (msyncFound()) {
      try {
        msyncMethod.invoke(getInstance());
      } catch (IllegalArgumentException | UnsupportedOperationException e) {
        LOG.debug("Failure of msync()", e);
      }
    }
  }

  @Override
  public boolean isImplemented(final String capability) {
    // keep new entries in alphanumeric order.
    switch (capability.toLowerCase(Locale.ROOT)) {
    case MSYNC:
      return msyncFound();
    case OPENFILE:
      return openFileFound();
    case PATH_CAPABILITIES:
      return pathCapabilitiesFound();
    default:
      return false;
    }
  }


  @Override
  public String toString() {
    return "FileSystemShimImpl{} "
        + availability(this, MSYNC, OPENFILE, PATH_CAPABILITIES)
        + super.toString();
  }

}
