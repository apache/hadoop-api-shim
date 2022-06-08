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

import static org.apache.hadoop.fs.shim.api.OpenFileConstants.FILESYSTEM_OPTION;

/**
 * Constants for the Shim classes themselves; not the APIs they shim.
 */
public class ShimConstants {

  public static final String FS_OPTION_SHIM =
      FILESYSTEM_OPTION + "shim.";

  /**
   * Should OpenFile be invoked at all?  {@value}.
   */
  public static final String FS_OPTION_SHIM_OPENFILE_ENABLED =
      FS_OPTION_SHIM + "openfile.enabled";

  /** Default value: {@value}. */
  public static final boolean FS_OPTION_SHIM_OPENFILE_ENABLED_DEFAULT = true;

}