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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.shim.api.VectorFileRange;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.ctor;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.loadInvocation;

/**
 * Class to bridge to a FileRange implementation class through reflection.
 */
public final class FileRangeBridge {
  private static final Logger LOG = LoggerFactory.getLogger(FileRangeBridge.class);

  public static final String CLASSNAME = "org.apache.hadoop.fs.impl.FileRangeImpl";

  private final Class<?> fileRangeClass;
  private final Invocation<Long> _getOffset;
  private final Invocation<Integer> _getLength;
  private final Invocation<CompletableFuture<ByteBuffer>> _getData;
  private final Invocation<Void> _setData;
  private final Invocation<Object> _getReference;
  private final Constructor<?> newFileRange;

  /**
   * Constructor.
   */
  public FileRangeBridge() {

    // try to load the class
    Class<?> cl;
    try {
      cl = this.getClass().getClassLoader().loadClass(CLASSNAME);
    } catch (ClassNotFoundException e) {
      LOG.debug("No {}", CLASSNAME);
      cl = null;
    }
    fileRangeClass = cl;
    // class found, so load the methods
    _getOffset = loadInvocation(fileRangeClass, "getOffset", Long.class);
    _getLength= loadInvocation(fileRangeClass, "getLength", Integer.class);
    _getData = loadInvocation(fileRangeClass, "getData", null);
    _setData = loadInvocation(fileRangeClass, "setData", Void.class, CompletableFuture.class);
    _getReference = loadInvocation(fileRangeClass, "getReference", Object.class);

    newFileRange = ctor(fileRangeClass, Long.class, Integer.class, Object.class);
  }

  /**
   * Is the bridge available.
   * @return true iff the bridge is present.
   */
  public boolean bridgeAvailable() {
    return fileRangeClass != null;
  }

  /**
   * Get the file range class.
   * @return the file range implementation class, if present.
   */
  public Class<?> getFileRangeClass() {
    return fileRangeClass;
  }

  /**
   * Instantiate.
   * @return a VectorFileRange wrapping a FileRange
   * @throws RuntimeException if the range cannot be instantiated
   * @throws IllegalStateException if the API is not available.
   */
  public WrappedFileRange newWrappedFileRange(long offset, int length) {
    Preconditions.checkState(bridgeAvailable(), "FileRange not available");
    try {
      return new WrappedFileRange(newFileRange.newInstance(offset, length));
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("failed to instantiate a FileRange object: " + e,
          e);
    }
  }

  /**
   * Convert a range to an instance of FileRange
   * @param in input
   * @return a converted instance
   */
  public Object toFileRange(VectorFileRange in) {
    // create a new wrapped file range, fill in and then
    // get the instance
    final WrappedFileRange wfr = newWrappedFileRange(in.getOffset(), in.getLength());
    wfr.setData(in.getData());
    return wfr.getInstance();
  }

  /**
   * This creates an implementation of {@link VectorFileRange} which
   * actually forwards to the inner FileRange class through reflection.
   * This allows the rest of the shim library to use the VectorFileRange
   * API to interact with these.
   */
  private class WrappedFileRange implements VectorFileRange {
    final Object instance;

    /**
     * Instantiate.
     * @param instance non null instance.
     */
    private WrappedFileRange(final Object instance) {
      this.instance = requireNonNull(instance);
    }

    @Override
    public long getOffset() {
      return _getOffset.invokeUnchecked(instance);
    }

    @Override
    public int getLength() {
      return _getLength.invokeUnchecked(instance);
    }

    @Override
    public CompletableFuture<ByteBuffer> getData() {
      return _getData.invokeUnchecked(instance);

    }

    @Override
    public void setData(final CompletableFuture<ByteBuffer> data) {
      _setData.invokeUnchecked(instance, data);
    }

    @Override
    public Object getReference() {
      return _getReference.invokeUnchecked(instance);
    }

    /**
     * Get the instance.
     * @return the instance.
     */
    public Object getInstance() {
      return instance;
    }
  }

}
