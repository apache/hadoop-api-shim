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

import org.apache.hadoop.fs.shim.api.ShimFeatureKeys;

public class Hadoop332Features extends Hadoop330Features {

  /**
   * Query for a feature being supported.
   * @param feature feature to query
   * @return true if the feature is supported
   */
  public boolean isImplemented(String feature) {
    switch (feature) {
    case ShimFeatureKeys.MSYNC:
      return true;
    default:
      return super.isImplemented(feature);
    }
  }
}
