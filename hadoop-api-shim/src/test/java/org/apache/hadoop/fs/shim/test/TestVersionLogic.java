/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.shim.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.shim.api.IsImplemented;

import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.BYTEBUFFER_POSITIONED_READ;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.IOSTATISTICS;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.MSYNC;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.OPENFILE;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.PATH_CAPABILITIES;
import static org.apache.hadoop.fs.shim.api.ShimFeatureKeys.VECTOR_IO;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.availability;

/**
 * Test Open operations.
 */
public class TestVersionLogic
    extends AbstractShimContractTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestVersionLogic.class);

  @Test
  public void testVersionLoad() throws Throwable {
    final IsImplemented version = getVersionCapabilities();
    LOG.info("Version {}", version);
    LOG.info("Features {}",
        availability(version,
            BYTEBUFFER_POSITIONED_READ,
            IOSTATISTICS,
            MSYNC,
            OPENFILE,
            PATH_CAPABILITIES,
            VECTOR_IO));

  }
}
