/**
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

/**
 *
 *  The Standard StreamCapabilities.
 */

public interface StandardStreamCapabilities {
  /**
   * Stream hflush capability implemented by {@code Syncable#hflush()}.
   *
   * Use the {@code #HSYNC} probe to check for the support of Syncable;
   * it's that presence of {@code hsync()} which matters.
   */
  @Deprecated
  String HFLUSH = "hflush";

  /**
   * Stream hsync capability implemented by {@code Syncable#hsync()}.
   */
  String HSYNC = "hsync";

  /**
   * Stream setReadahead capability implemented by
   * {@code CanSetReadahead#setReadahead(Long)}.
   */
  String READAHEAD = "in:readahead";

  /**
   * Stream setDropBehind capability implemented by
   * {@code CanSetDropBehind#setDropBehind(Boolean)}.
   */
  String DROPBEHIND = "dropbehind";

  /**
   * Stream unbuffer capability implemented by {@code CanUnbuffer#unbuffer()}.
   */
  String UNBUFFER = "in:unbuffer";

  /**
   * Stream read(ByteBuffer) capability implemented by
   * {@code ByteBufferReadable#read(java.nio.ByteBuffer)}.
   */
  String READBYTEBUFFER = "in:readbytebuffer";

  /**
   * Does the stream support accelerated vectored reads?
   * The API is <i>always</i> available; this is about
   * whether or not the stream offers a custom, performance
   * implementation.
   */
  String READVECTORED = "in:readvectored";

  /**
   * Stream read(long, ByteBuffer) capability implemented by
   * {@code ByteBufferPositionedReadable#read(long, java.nio.ByteBuffer)}.
   * This is always offered by the {@link FSDataInputStreamShim} class.
   */
  String PREADBYTEBUFFER = "in:preadbytebuffer";

  /**
   * IOStatisticsSource API.
   * There's no shim support as it is just too complicated
   * in terms of datatypes.
   */
  String IOSTATISTICS = "iostatistics";

  /**
   * Streams that support IOStatistics context and capture thread-level
   * IOStatistics.
   */
  String IOSTATISTICS_CONTEXT = "fs.capability.iocontext.supported";

  /**
   * Flag to indicate whether a stream is a magic output stream;
   * returned in {@code StreamCapabilities}
   * Value: {@value}.
   */
  String S3A_STREAM_CAPABILITY_MAGIC_OUTPUT
      = "fs.s3a.capability.magic.output.stream";

  /**
   * Flag to indicate that a store supports magic committers.
   * returned in {@code PathCapabilities}
   * Value: {@value}.
   */
  String S3A_CAPABILITY_MAGIC_COMMITTER
      = "fs.s3a.capability.magic.committer";

  /**
   * {@code PathCapabilities} probe to indicate that the filesystem
   * keeps directory markers.
   * Value: {@value}.
   */
  String S3A_CAPABILITY_DIRECTORY_MARKER_POLICY_KEEP
      = "fs.s3a.capability.directory.marker.policy.keep";

  /**
   * {@code PathCapabilities} probe to indicate that the filesystem
   * deletes directory markers.
   * Value: {@value}.
   */
  String S3A_CAPABILITY_DIRECTORY_MARKER_POLICY_DELETE
      = "fs.s3a.capability.directory.marker.policy.delete";

  /**
   * {@code PathCapabilities} probe to indicate that the filesystem
   * keeps directory markers in authoritative paths only.
   * Value: {@value}.
   */
  String
      S3A_CAPABILITY_DIRECTORY_MARKER_POLICY_AUTHORITATIVE =
      "fs.s3a.capability.directory.marker.policy.authoritative";

  /**
   * {@code PathCapabilities} probe to indicate that a path
   * keeps directory markers.
   * Value: {@value}.
   */
  String S3A_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP
      = "fs.s3a.capability.directory.marker.action.keep";

  /**
   * {@code PathCapabilities} probe to indicate that a path
   * deletes directory markers.
   * Value: {@value}.
   */
  String S3A_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE
      = "fs.s3a.capability.directory.marker.action.delete";

  
  /**
   * Does this version of the store have safe readahead?
   * Possible combinations of this and the probe
   * {@code "fs.capability.etags.available"}.
   * <ol>
   *   <li>{@value}: store is safe</li>
   *   <li>no etags: store is safe</li>
   *   <li>etags and not {@value}: store is <i>UNSAFE</i></li>
   * </ol>
   */
  String CAPABILITY_SAFE_READAHEAD =
      "fs.azure.capability.readahead.safe";
  
}

