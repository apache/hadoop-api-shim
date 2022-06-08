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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;

/**
 * LocalFSContract with the xml contract referenced from the
 * main jar, so as to ensure it is packaged and therefore accessible
 * in the separate test modules.
 *
 */
public class FileContract extends LocalFSContract {

  public FileContract(final Configuration conf) {
    super(conf);
  }

  @Override
  protected String getContractXml() {
    return "org/apache/hadoop/fs/shim/test/localfs.xml";
  }
}
