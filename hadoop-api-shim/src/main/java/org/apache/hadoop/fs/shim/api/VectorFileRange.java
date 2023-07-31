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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.shim.impl.VectorFileRangeImpl;

/**
 * A byte range of a file, based on the Hadoop Vector API class
 * {@code org.apache.hadoop.fs.FileRange} used in
 *  {@code PositionedReadable.readVectored()}.
 * In the shim code, it is used in for the vector read operation
 * {@link FSDataInputStreamShim#readVectoredRanges(List, IntFunction)}.
 */
public interface VectorFileRange {

  /**
   * Get the starting offset of the range.
   *
   * @return the byte offset of the start
   */
  long getOffset();

  /**
   * Get the length of the range.
   *
   * @return the number of bytes in the range.
   */
  int getLength();

  /**
   * Get the future data for this range.
   *
   * @return the future for the {@link ByteBuffer} that contains the data
   */
  CompletableFuture<ByteBuffer> getData();

  /**
   * Set a future for this range's data.
   * This method is called by {@code PositionedReadable#readVectored} to store the
   * data for the user to pick up later via {@link #getData}.
   *
   * @param data the future of the ByteBuffer that will have the data
   */
  void setData(CompletableFuture<ByteBuffer> data);

  /**
   * Get any reference passed in to the file range constructor.
   * This is not used by any implementation code; it is to help
   * bind this API to libraries retrieving multiple stripes of
   * data in parallel.
   *
   * @return a reference or null.
   */
  Object getReference();

  /**
   * Get the wrapped instance.
   * Return null if it is not actually wrapping anything.
   * @return wrapped object.
   */
  Object getInstance();

  /**
   *
   * Factory method to create a VectorFileRange object.
   *
   * @param offset starting offset of the range.
   * @param length length of the range.
   *
   * @return a new instance of FileRangeImpl.
   */
  static VectorFileRange createFileRange(long offset, int length) {
    return new VectorFileRangeImpl(offset, length, null);
  }

  /**
   * Factory method to create a VectorFileRange object.
   *
   * @param offset starting offset of the range.
   * @param length length of the range.
   * @param reference nullable reference to store in the range.
   *
   * @return a new instance of FileRangeImpl.
   */
  static VectorFileRange createFileRange(long offset, int length, Object reference) {
    return new VectorFileRangeImpl(offset, length, reference);
  }
}
