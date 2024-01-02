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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.shim.api.VectorFileRange;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.loadInvocation;

/**
 * Class to bridge to a FileRange implementation class through reflection.
 */
public final class FileRangeBridge {
  private static final Logger LOG = LoggerFactory.getLogger(FileRangeBridge.class);

  /**
   * Name of the {@code FileRange} interface.
   */
  public static final String CLASSNAME = "org.apache.hadoop.fs.FileRange";

  /**
   * Class of the interface {@link #CLASSNAME}, if loaded.
   * This can resolve all methods in this interface and super-interfaces,
   * including static ones.
   */
  private final Class<?> fileRangeInterface;
  private final Invocation<Long> _getOffset;
  private final Invocation<Integer> _getLength;
  private final Invocation<CompletableFuture<ByteBuffer>> _getData;
  private final Invocation<Void> _setData;
  private final Invocation<Object> _getReference;

  /**
   * new FileRange(long, long, Object)
   */
  private final Invocation<Object> createFileRange;

  /**
   * Constructor.
   */
  public FileRangeBridge() {

    // try to load the class
    fileRangeInterface = ShimReflectionSupport.loadClass(CLASSNAME);
    // class found, so load the methods
    _getOffset = loadInvocation(fileRangeInterface, long.class, "getOffset");
    _getLength = loadInvocation(fileRangeInterface, int.class, "getLength");
    _getData = loadInvocation(fileRangeInterface, null, "getData");
    _setData = loadInvocation(fileRangeInterface, void.class, "setData", CompletableFuture.class);
    _getReference = loadInvocation(fileRangeInterface, Object.class, "getReference");

    // static interface method to create an instance.
    createFileRange = loadInvocation(fileRangeInterface, Object.class, "createFileRange", long.class,
        int.class, Object.class);

  }


  /**
   * Is the bridge available.
   *
   * @return true iff the bridge is present.
   */
  public boolean bridgeAvailable() {
    return fileRangeInterface != null;
  }

  /**
   * Get the file range class.
   *
   * @return the file range implementation class, if present.
   */
  public Class<?> getFileRangeInterface() {
    return fileRangeInterface;
  }

  /**
   * Instantiate.
   *
   * @param offset offset in file
   * @param length length of data to read.
   * @param reference nullable reference to store in the range.
   * @return a VectorFileRange wrapping a FileRange
   *
   * @throws RuntimeException if the range cannot be instantiated
   * @throws IllegalStateException if the API is not available.
   */
  public VectorFileRange createFileRange(long offset, int length, final Object reference) {
    Preconditions.checkState(bridgeAvailable(), "FileRange not available");
    return new WrappedFileRange(createFileRange.invokeUnchecked(null, offset, length, reference));
  }

  /**
   * Convert a range to an instance of FileRange.
   * The offset, length and reference of the input range
   * all passed into the createFileRange() method.
   * @param range input range
   *
   * @return a converted instance
   */
  public Object toFileRange(VectorFileRange range) {
    // create a new wrapped file range, fill in and then
    // get the instance
    final VectorFileRange wfr = createFileRange(
        range.getOffset(), range.getLength(), range.getReference());
    return wfr.getInstance();
  }

  /**
   * This creates an implementation of {@link VectorFileRange} which
   * actually forwards to the inner FileRange class through reflection.
   * This allows the rest of the shim library to use the VectorFileRange
   * API to interact with these.
   */
  private class WrappedFileRange implements VectorFileRange {

    /**
     * The wrapped range.
     */
    private final Object instance;

    /**
     * Instantiate.
     *
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
     *
     * @return the instance.
     */
    public Object getInstance() {
      return instance;
    }
  }

}
