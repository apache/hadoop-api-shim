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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.shim.api.FSDataInputStreamShim;
import org.apache.hadoop.fs.shim.api.VectorFileRange;

import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.BYTEBUFFER_POSITIONED_READ;
import static org.apache.hadoop.fs.shim.api.StandardStreamCapabilities.PREADBYTEBUFFER;
import static org.apache.hadoop.fs.shim.api.StandardStreamCapabilities.READVECTORED;
import static org.apache.hadoop.fs.shim.impl.Invocation.unavailable;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.availability;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.loadInvocation;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * Extend FS implementations with
 * <ol>
 *   <li>implementation of ByteBufferPositionedReadable</li>
 *   <li>readVectoredRanges()</li>
 * </ol>
 *
 * There's an expectation that the stream implements readFully() efficienty, and
 * is has a lazy seek() call, or the cost of a seek() is so low as to not matter.
 */
public class FSDataInputStreamShimImpl
    extends AbstractAPIShim<FSDataInputStream>
    implements FSDataInputStreamShim {

  private static final Logger LOG = LoggerFactory.getLogger(FSDataInputStreamShimImpl.class);

  /**
   * buffer size for fallbacks when reading into ByteBuffers which are not also
   * arrays.
   * TODO: make configurable?
   */
  public static final int TEMPORARY_BUFFER = 1024 * 128;
  public static final String READ_FULLY = "readFully";
  public static final String READ = "read";
  public static final String READ_VECTORED = "readVectored";

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation<Integer> byteBufferPositionedRead;

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation<Void> byteBufferPositionedReadFully;
  private final AtomicBoolean isByteBufferPositionedReadAvailable;

  private final AtomicBoolean isByteBufferReadableAvailable;

  /**
   * FileRange class. This could be shared.
   */
  private static final FileRangeBridge FILE_RANGE_BRIDGE = new FileRangeBridge();

  /**
   * readVectored() API.
   * If present will be invoked without any fallback.
   */
  private final Invocation<Void> readVectored;

  /**
   * Constructor.
   *
   * @param instance Instance being shimmed.
   */
  public FSDataInputStreamShimImpl(
      final FSDataInputStream instance) {
    super(FSDataInputStream.class, instance);
    byteBufferPositionedRead = loadInvocation(getClazz(), READ,
        Integer.class,
        Long.class, ByteBuffer.class);

    boolean bbrb = instance.hasCapability(PREADBYTEBUFFER)
        && byteBufferPositionedRead.available();
    if (bbrb) {
      byteBufferPositionedReadFully = loadInvocation(getClazz(),
          READ_FULLY, Void.class, Long.class, ByteBuffer.class);
      isByteBufferPositionedReadAvailable = new AtomicBoolean(true);
    } else {
      byteBufferPositionedReadFully = unavailable(READ_FULLY);
      isByteBufferPositionedReadAvailable = new AtomicBoolean(false);
    }
    // declare ByteBufferReadable available if the inner stream supports it.
    // if an attempt to use it fails, it will downgrade
    isByteBufferReadableAvailable = new AtomicBoolean(
        instance.getWrappedStream() instanceof ByteBufferReadable);
    if (FILE_RANGE_BRIDGE.bridgeAvailable()) {
      readVectored = loadInvocation(getClazz(), READ_VECTORED,
          Void.class, List.class, Function.class);
    } else {
      readVectored = unavailable(READ_VECTORED);
    }

  }

  @Override
  public boolean hasCapability(final String capability) {
    switch (toLowerCase(capability)) {
    case PREADBYTEBUFFER:
      // positioned read is always available
      return true;
    case READVECTORED:
      return isVectorReadAvailable();
    default:
      return getInstance().hasCapability(capability);
    }
  }

  @Override
  public boolean isImplemented(final String capability) {
    switch (capability.toLowerCase(Locale.ROOT)) {
    case PREADBYTEBUFFER:
      return isByteBufferPositionedReadAvailable();
    case READVECTORED:
      return isVectorReadAvailable();
    default:
      return false;
    }
  }

  @Override
  public String toString() {
    return "FSDataInputStreamShimImpl{} "
        + availability(this, BYTEBUFFER_POSITIONED_READ, READVECTORED)
        + super.toString();
  }

  @Override
  public int read(final long position, final byte[] buffer, final int offset, final int length)
      throws IOException {
    return getInstance().read(position, buffer, offset, length);
  }

  @Override
  public void readFully(final long position,
      final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    getInstance().readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(final long position, final byte[] buffer) throws IOException {
    getInstance().readFully(position, buffer);
  }

  /**
   * ReadVectored acceleration available if the API is loaded
   * and the instance says it supports it
   *
   * @return true iff the stream implements it.
   */
  public final boolean isVectorReadAvailable() {
    return readVectored.available() && getInstance().hasCapability(READVECTORED);
  }

  @Override
  public final boolean isByteBufferPositionedReadAvailable() {
    return isByteBufferPositionedReadAvailable.get();
  }

  @Override
  public int read(long position, ByteBuffer buf) throws IOException {
    if (isByteBufferPositionedReadAvailable()) {
      try {
        return byteBufferPositionedRead.invoke(getInstance(), position, buf);
      } catch (UnsupportedOperationException e) {
        LOG.debug("Failure to invoke read() on {}", getInstance(), e);
        // note the api isn't actually available,
        // before falling back.
        isByteBufferPositionedReadAvailable.set(false);
      }
    }
    fallbackByteBufferRead(position, buf);
    return byteBufferPositionedRead.invoke(getInstance(), position, buf);
  }

  @Override
  public void readFully(long position, ByteBuffer buf) throws IOException {
    if (isByteBufferPositionedReadAvailable()) {
      try {
        byteBufferPositionedReadFully.invoke(getInstance(), position, buf);
        return;
      } catch (UnsupportedOperationException e) {
        LOG.debug("Failure to invoke readFully() on {}", getInstance(), e);
        // note the api isn't actually available,
        // before falling back.
        isByteBufferPositionedReadAvailable.set(false);
      }
    }
    fallbackByteBufferReadFully(position, buf);
  }

  /**
   * Fallback implementation of PositionedReadable: read into a buffer
   * Based on some of the hdfs code.
   * {@code DFSInputStream.actualGetFromOneDataNode()}.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   *
   * @throws IOException failure
   */
  private synchronized void fallbackByteBufferReadFully(long position, ByteBuffer buf)
      throws IOException {
    FSDataInputStream in = getInstance();
    int len = buf.remaining();
    LOG.debug("read @{} {} bytes", position, len);
    // position to return to.
    if (buf.hasArray()) {
      readIntoArrayByteBufferThroughReadFully(position, buf, len);
      return;
    }
    // no array.
    // is the inner stream ByteBufferReadable? if so, read
    // through that then seek back.
    if (isByteBufferReadableAvailable.get()) {
      LOG.debug("reading bytebuffer through seek and read(ByteBuffer)");
      try (SeekToThenBack back = new SeekToThenBack(position)) {
        while (buf.remaining() > 0) {
          int bytesRead = in.read(buf);
          if (bytesRead < 0) {
            throw new EOFException("No more data in stream; needed "
                + buf.remaining() + " to complete the read");
          }
        }
        return;
      } catch (UnsupportedOperationException ex) {
        LOG.debug("stream does not support ByteBufferReadable", ex);
        // don't try using this again
        isByteBufferReadableAvailable.set(false);
        /* and fall through into the final strategy */
      }
    }

    // final strategy.
    // buffer isn't an array, so need to create a smaller one then read via a series of readFully
    // calls.
    LOG.debug("Reading the byte buffer by reading into an array and copying");
    int bufferSize = Math.min(len, TEMPORARY_BUFFER);
    byte[] byteArray = new byte[bufferSize];
    long nextReadPosition = position;
    while (buf.remaining() > 0) {
      int bytesToRead = Math.min(bufferSize, buf.remaining());
      LOG.debug("Reading {} bytes from {}", bytesToRead, nextReadPosition);
      getInstance().readFully(nextReadPosition, byteArray, 0,
          bytesToRead);
      buf.put(byteArray, 0, bytesToRead);
      // move forward in the file
      nextReadPosition += bytesToRead;
    }

  }

  /**
   * Read directly into bytebuffer array via PositionedReadable.readFully()");
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @param len length of data to read
   *
   * @throws IOException failure
   */
  private void readIntoArrayByteBufferThroughReadFully(
      final long position,
      final ByteBuffer buf,
      final int len) throws IOException {
    LOG.debug("reading directly into bytebuffer array via PositionedReadable.readFully()");
    ByteBuffer tmp = buf.duplicate();
    tmp.limit(tmp.position() + len);
    tmp = tmp.slice();
    readFully(position, tmp.array(), tmp.position(), len);
    buf.position(buf.position() + len);
  }

  /**
   * Fallback implementation of PositionedReadable: read into a buffer
   * Based on some of the hdfs code.
   * {@code DFSInputStream.actualGetFromOneDataNode()}.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   *
   * @return bytes read
   *
   * @throws IOException failure
   */
  private synchronized int fallbackByteBufferRead(long position, ByteBuffer buf)
      throws IOException {
    int len = buf.remaining();
    // position to return to.
    if (buf.hasArray()) {
      ByteBuffer tmp = buf.duplicate();
      tmp.limit(tmp.position() + len);
      tmp = tmp.slice();
      int read = read(position, tmp.array(), tmp.position(), len);
      buf.position(buf.position() + read);
      return read;
    } else {
      // only read up to the temp buffer; caller gets to
      // ask for more if it they want it
      int bufferSize = Math.min(len, TEMPORARY_BUFFER);
      byte[] byteArray = new byte[bufferSize];
      int read = read(position, byteArray, 0, bufferSize);
      buf.put(byteArray, 0, bufferSize);
      return read;
    }

  }

  /**
   * class to seek back to the original position after a read;
   * intended for use in try with closeable.
   */
  private final class SeekToThenBack implements Closeable {

    /**
     * Original position; this will be returned to in close.
     */
    private final long pos;

    public SeekToThenBack(long newPos) throws IOException {
      this.pos = getInstance().getPos();
      seekTo(newPos);
    }

    /**
     * On demand seek.
     *
     * @param newPos new position
     *
     * @throws IOException failure.
     */
    private void seekTo(long newPos) throws IOException {
      if (getInstance().getPos() != newPos) {
        getInstance().seek(newPos);
      }
    }

    /**
     * Seek back to the original position if needed.
     *
     * @throws IOException failure
     */
    @Override
    public void close() throws IOException {
      seekTo(pos);
    }

  }

  /**
   * Declaration of the readVectored API.
   * This is special in that the type of the list doesn't exist at compile time;
   * it relies on type erasure to publish an interface with the exact same runtime
   * signature as {@code PositionedReadable.readVectored(List<FileRange>}.
   *
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   *
   * @throws IOException any IOE.
   * @throws UnsupportedOperationException if invoked on older releases.
   */
  private void invokeReadVectored(List<?> ranges,
      IntFunction<ByteBuffer> allocate) throws IOException {

    // if the api is available on PositionedReadable, invoke it.
    // if it isn't, this method can only be invoked on direct
    // access to FSDataInputStreamShimImpl. So fail.
    readVectored.invoke(getInstance(), ranges, allocate);
  }

  /**
   * The shim method, which will invoke readVectored() if present,
   * and fallback to byte buffer/positioned read calls if not.
   *
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   *
   * @throws IOException IO failure.
   */

  @Override
  public void readVectoredRanges(
      List<VectorFileRange> ranges,
      IntFunction<ByteBuffer> allocate)
      throws IOException {

    // if the readRange API is present, convert the arguments and delegate.
    if (readVectored.available()) {
      final List<Object> list = ranges.stream()
          .map(FILE_RANGE_BRIDGE::toFileRange)
          .collect(Collectors.toList());
      invokeReadVectored(list, allocate);
    } else {
      // one of ranges.
      // fallback code
      VectoredRangeReadImpl.readRanges(this,
          isByteBufferPositionedReadAvailable(),
          ranges,
          allocate);
    }
  }

}
