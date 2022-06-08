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

package org.apache.hadoop.fs.shim.test.binding;

import org.apache.hadoop.fs.shim.api.IsImplemented;

/**
 * Rather than do things with XML resources the way
 * contract tests do, have a class which implements
 * StreamCapabilities.
 * This will be implemented as a tree of previous
 * releases, on the assumption that capabilities
 * don't get removed.
 * Note that there are some features in hadoop 3.3.2
 * not in 3.3.0.
 */
public class Hadoop320Features implements IsImplemented {

  /**
   * Hadoop 3.2.0 returns "false" for all capabilities.
   * @param capability capability/feature to probe for
   *
   * @return false, always.
   */
  @Override
  public boolean isImplemented(final String capability) {
    return false;
  }
}
