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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Constants from recent hadoop releases.
 */
public class OpenFileConstants {

  /**
   * Read policy for adaptive IO: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE =
      "adaptive";

  /**
   * Read policy {@value} -whateve the implementation does by default.
   */
  public static final String FS_OPTION_OPENFILE_READ_POLICY_DEFAULT =
      "default";

  /**
   * Read policy for random IO: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_READ_POLICY_RANDOM =
      "random";

  /**
   * Read policy for sequential IO: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL =
      "sequential";

  /**
   * Read policies supported by the s3a connector in hadoop 3.2+
   * The public open file option adds more and the ability to
   * provide an ordered list of preferred policies.
   * As s3a connectors without that feature can't handle
   * a list, this set is used to identify which options can be
   * safely passed down.
   */
  static final Set<String> S3A_READ_POLICIES =
      Collections.unmodifiableSet(Stream.of(
              FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE,
              FS_OPTION_OPENFILE_READ_POLICY_DEFAULT,
              FS_OPTION_OPENFILE_READ_POLICY_RANDOM,
              FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)
          .collect(Collectors.toSet()));

  public static final String S3A_INPUT_FADVISE =
      "fs.s3a.experimental.input.fadvise";


  public static final String S3A_READAHEAD_RANGE = "fs.s3a.readahead.range";

  /**
   * Prefix for all standard filesystem options: {@value}.
   */
  public static final String FILESYSTEM_OPTION = "fs.option.";

  /**
   * Prefix for all openFile options: {@value}.
   */
  public static final String FS_OPTION_OPENFILE =
      FILESYSTEM_OPTION + "openfile.";

  /**
   * OpenFile option for read policies: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_READ_POLICY =
      FS_OPTION_OPENFILE + "read.policy";

  /**
   * OpenFile option for buffer size: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_BUFFER_SIZE =
      FS_OPTION_OPENFILE + "buffer.size";

  /**
   * OpenFile option for split end: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_SPLIT_END =
      FS_OPTION_OPENFILE + "split.end";

  /**
   * OpenFile option for split start: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_SPLIT_START =
      FS_OPTION_OPENFILE + "split.start";

  /**
   * OpenFile option for file length: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_LENGTH =
      FS_OPTION_OPENFILE + "length";


  /**
   * openFile option to enable a lock free pread which will bypass buffer in AbfsInputStream.
   * {@value}.
   */
  public static final String ABFS_BUFFERED_PREAD_DISABLE = "fs.azure.buffered.pread.disable";

}
