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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.shim.api.FSDataInputStreamShim;
import org.apache.hadoop.fs.shim.api.VectorFileRange;
import org.apache.hadoop.fs.shim.functional.Function4RaisingIOE;

/**
 * Utility class which implements helper methods used
 * in vectored IO implementation.
 */
public final class VectoredRangeReadImpl {

  private static final int TMP_BUFFER_MAX_SIZE = 64 * 1024;

  /**
   * private constructor.
   */
  private VectoredRangeReadImpl() {
    throw new UnsupportedOperationException();
  }

  /**
   * Validate a single range.
   *
   * @param range file range.
   *
   * @throws EOFException any EOF Exception.
   */
  public static void validateRangeRequest(VectorFileRange range)
      throws EOFException {

    Preconditions.checkArgument(range.getLength() >= 0, "length is negative");
    if (range.getOffset() < 0) {
      throw new EOFException("position is negative");
    }
  }

  /**
   * Validate a list of vectored read ranges.
   *
   * @param ranges list of ranges.
   *
   * @throws EOFException any EOF exception.
   */
  public static void validateVectoredReadRanges(List<? extends VectorFileRange> ranges)
      throws EOFException {
    for (VectorFileRange range : ranges) {
      validateRangeRequest(range);
    }
  }

  /**
   * Read bytes from stream into a byte buffer using an
   * intermediate byte array.
   *
   * @param length number of bytes to read.
   * @param buffer buffer to fill.
   * @param operation operation to use for reading data.
   *
   * @throws IOException any IOE.
   */
  public static void readInDirectBuffer(int length,
      ByteBuffer buffer,
      Function4RaisingIOE<Integer, byte[], Integer, Integer, Void> operation)
      throws IOException {
    if (length == 0) {
      return;
    }
    int readBytes = 0;
    int position = 0;
    int tmpBufferMaxSize = Math.min(TMP_BUFFER_MAX_SIZE, length);
    byte[] tmp = new byte[tmpBufferMaxSize];
    while (readBytes < length) {
      int currentLength = (readBytes + tmpBufferMaxSize) < length
          ? tmpBufferMaxSize
          : (length - readBytes);
      operation.apply(position, tmp, 0, currentLength);
      buffer.put(tmp, 0, currentLength);
      position = position + currentLength;
      readBytes = readBytes + currentLength;
    }
  }


  /**
   * Check if the input ranges are overlapping in nature.
   * We call two ranges to be overlapping when start offset
   * of second is less than the end offset of first.
   * End offset is calculated as start offset + length.
   *
   * @param input list if input ranges.
   *
   * @return true/false based on logic explained above.
   */
  public static List<? extends VectorFileRange> validateNonOverlappingAndReturnSortedRanges(
      List<? extends VectorFileRange> input) throws EOFException {
    validateVectoredReadRanges(input);
    if (input.size() <= 1) {
      return input;
    }
    VectorFileRange[] sortedRanges = sortRanges(input);
    VectorFileRange prev = sortedRanges[0];
    for (int i = 1; i < sortedRanges.length; i++) {
      if (sortedRanges[i].getOffset() < prev.getOffset() + prev.getLength()) {
        throw new UnsupportedOperationException("Overlapping ranges are not supported");
      }
      prev = sortedRanges[i];
    }
    return Arrays.asList(sortedRanges);
  }

  /**
   * Sort the input ranges by offset.
   *
   * @param input input ranges.
   *
   * @return sorted ranges.
   */
  public static VectorFileRange[] sortRanges(List<? extends VectorFileRange> input) {
    VectorFileRange[] sortedRanges = input.toArray(new VectorFileRange[0]);
    Arrays.sort(sortedRanges, Comparator.comparingLong(VectorFileRange::getOffset));
    return sortedRanges;
  }

  /**
   * Read a range through the PositionedReadable interface.
   *
   * @param in input
   * @param range range
   * @param buffer buffer to read into.
   *
   * @throws IOException IO failuer
   */
  public static void readRangeThroughPositionedReadable(
      PositionedReadable in,
      VectorFileRange range,
      ByteBuffer buffer) throws IOException {

    if (buffer.isDirect()) {
      readInDirectBuffer(range.getLength(),
          buffer,
          (position, buffer1, offset, length) -> {
            in.readFully(position, buffer1, offset, length);
            return null;
          });
      buffer.flip();
    } else {
      in.readFully(range.getOffset(), buffer.array(),
          buffer.arrayOffset(), range.getLength());
    }
  }

  /**
   * Iterates through the ranges
   * to read each synchronously.
   * The data or exceptions are pushed into {@link VectorFileRange#getData()}.
   *
   * @param in input stream
   * @param useByteBufferPositionedRead useByteBufferPositionedReadable API
   * @param ranges the byte ranges to read
   * @param allocate the byte buffer allocation
   */
  static void readRanges(
      final FSDataInputStreamShim in,
      boolean useByteBufferPositionedRead,
      List<? extends VectorFileRange> ranges,
      IntFunction<ByteBuffer> allocate) throws EOFException {
    for (VectorFileRange range : validateNonOverlappingAndReturnSortedRanges(ranges)) {
      range.setData(readOneRange(in, useByteBufferPositionedRead, range, allocate));
    }
  }

  /**
   * Synchronously reads a range from the stream dealing with the combinations
   * of ByteBuffers buffers and PositionedReadable streams.
   *
   * @param in input stream
   * @param useByteBufferPositionedRead useByteBufferPositionedReadable API
   * @param range the range to read
   * @param allocate the function to allocate ByteBuffers
   *
   * @return the CompletableFuture that contains the read data
   */
  static CompletableFuture<ByteBuffer> readOneRange(
      FSDataInputStreamShim in,
      boolean useByteBufferPositionedRead,
      VectorFileRange range,
      IntFunction<ByteBuffer> allocate) {
    CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
    try {
      ByteBuffer buffer = allocate.apply(range.getLength());
      if (useByteBufferPositionedRead) {
        // use readFully if present
        in.readFully(range.getOffset(), buffer);
        buffer.flip();
      } else {
        readRangeThroughPositionedReadable(in, range, buffer);
      }
      result.complete(buffer);
    } catch (IOException ioe) {
      result.completeExceptionally(ioe);
    }
    return result;
  }

}
