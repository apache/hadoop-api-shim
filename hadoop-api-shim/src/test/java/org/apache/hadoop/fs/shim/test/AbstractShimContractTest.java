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

package org.apache.hadoop.fs.shim.test;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.shim.api.FileSystemShim;
import org.apache.hadoop.fs.shim.api.IsImplemented;
import org.apache.hadoop.fs.shim.api.ShimFactory;
import org.apache.hadoop.fs.shim.test.binding.FileContract;
import org.apache.hadoop.fs.shim.test.binding.Hadoop320Features;
import org.apache.hadoop.util.VersionInfo;

import static java.util.Objects.requireNonNull;

/**
 * Abstract FS contract test.
 * This implementation always returns the local FS as the contract, though
 * it can be overridden.
 */
public class AbstractShimContractTest extends AbstractFSContractTestBase
    implements StreamCapabilities {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractShimContractTest.class);

  private IsImplemented versionCapabilities;

  private FileSystemShim fsShim;

  public AbstractShimContractTest() {
    // versionCapabilities = new Hadoop320Features();
  }

  @BeforeClass
  public static void logHadoopVersion() {
    LOG.info("Hadoop version {}", VersionInfo.getBuildVersion());
  }

  public IsImplemented getVersionCapabilities() {
    return versionCapabilities;
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    FileContract contract = new FileContract(conf);
    return contract;
  }

  @Override
  public boolean hasCapability(final String capability) {
    return versionCapabilities.isImplemented(capability);
  }

  public FileSystemShim getFsShim() {
    return fsShim;
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    // also do the binding stuff here
    Configuration bindingConf = new Configuration(true);
    Class<? extends IsImplemented> binding = requireNonNull(
        bindingConf.getClass("hadoop.test.binding", Hadoop320Features.class,
            IsImplemented.class));
    versionCapabilities = binding.getConstructor().newInstance();
    LOG.info("Using version capability {}", versionCapabilities);
    fsShim = ShimFactory.shimFileSystem(getFileSystem());
  }

  /**
   * report a test has been skipped for some reason.
   *
   * @param message message to use in the text
   *
   * @throws AssumptionViolatedException always
   */
  public static void skip(String message) {
    LOG.info("Skipping: {}", message);
    throw new AssumptionViolatedException(message);
  }

  /**
   * Require the shim to implement a feature on the enabled run.
   *
   * @param capability capability
   */
  protected void requireImplementationIfVersionClaimsSupport(IsImplemented shim,
      final String capability,
      boolean load) {
    LOG.info("Shim is {}", shim);
    final boolean implemented = shim.isImplemented(capability);
    final IsImplemented capabilities = getVersionCapabilities();
    boolean expectImplementation = capabilities.isImplemented(capability);
    if (load) {
      // loading requested, make sure the asserts match
      if (expectImplementation) {
        Assertions.assertThat(implemented)
            .describedAs("implementation of %s by %s with" +
                "expectation set by %s", capability, shim, capabilities)
            .isTrue();
      } else {
        skip("Runtime does not have implementation");
      }
    }
  }
}
