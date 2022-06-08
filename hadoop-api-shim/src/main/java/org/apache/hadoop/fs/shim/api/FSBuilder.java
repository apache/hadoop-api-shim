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

import java.io.IOException;

/**
 * The base interface which various FileSystem FileContext Builder
 * interfaces can extend, and which underlying implementations
 * will then implement.
 * taken from {@code org.apache.hadoop.fs.FSBuilder}
 * @param <S> Return type on the {@link #build()} call.
 * @param <B> type of builder itself.
 */

public interface FSBuilder<S, B extends FSBuilder<S, B>> {

  /**
   * Set optional Builder parameter.
   * @param key key.
   * @param value value.
   * @return generic type B.
   */
  B opt(String key, String value);

  /**
   * Set optional boolean parameter for the Builder.
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  B opt(String key, boolean value);

  /**
   * Set optional int parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  B opt(String key, int value);

  /**
   * Set optional long parameter for the Builder.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #opt(String, String)
   */
  B optLong(String key, long value);

  /**
   * Set mandatory option to the Builder.
   *
   * If the option is not supported or unavailable,
   * the client should expect {@link #build()} throws IllegalArgumentException.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   */
  B must(String key, String value);

  /**
   * Set mandatory boolean option.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  B must(String key, boolean value);

  /**
   * Set mandatory int option.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  B must(String key, int value);

  /**
   * Set mandatory long option.
   *
   * @param key key.
   * @param value value.
   * @return generic type B.
   * @see #must(String, String)
   */
  B mustLong(String key, long value);


  /**
   * Instantiate the object which was being built.
   *
   * @throws IllegalArgumentException if the parameters are not valid.
   * @throws UnsupportedOperationException if the filesystem does not support
   * the specific operation.
   * @throws IOException on filesystem IO errors.
   * @return generic type S.
   */
  S build() throws IllegalArgumentException,
      UnsupportedOperationException, IOException;
}
