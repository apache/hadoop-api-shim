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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.api.FSBuilder;

import static org.apache.hadoop.fs.shim.impl.Preconditions.checkArgument;

/**
 * Builder for filesystem/filecontext operations of various kinds,
 * with option support.
 * <p>
 * <code>
 * .opt("foofs:option.a", true)
 * .opt("foofs:option.b", "value")
 * .opt("fs.s3a.open.option.etag", "9fe4c37c25b")
 * .must("foofs:cache", true)
 * .must("barfs:cache-size", 256 * 1024 * 1024)
 * .build();
 * </code>
 * <p>
 * Configuration keys declared in an {@code opt()} may be ignored by
 * a builder which does not recognise them.
 * <p>
 * Configuration keys declared in a {@code must()} function set must
 * be understood by the implementation or a
 * {@link IllegalArgumentException} will be thrown.
 *
 * @param <S> Return type on the {@link #build()} call.
 * @param <B> type of builder itself.
 */

public abstract class
AbstractFSBuilderImpl<S, B extends FSBuilder<S, B>>
    implements FSBuilder<S, B> {

  public static final String UNKNOWN_MANDATORY_KEY = "Unknown mandatory key";

  static final String E_BOTH_A_PATH_AND_A_PATH_HANDLE
      = "Both a path and a pathHandle has been provided to the constructor";

  private final Optional<Path> optionalPath;

  /**
   * Contains optional and mandatory parameters.
   *
   * It does not load default configurations from default files.
   */
  private final Configuration options = new Configuration(false);

  /** Keep track of the keys for mandatory options. */
  private final Set<String> mandatoryKeys = new HashSet<>();

  /** Keep track of the optional keys. */
  private final Set<String> optionalKeys = new HashSet<>();

  /**
   * Constructor with both optional path and path handle.
   * Either or both argument may be empty, but it is an error for
   * both to be defined.
   *
   * @param optionalPath a path or empty
   * @param optionalPathHandle a path handle/empty
   *
   * @throws IllegalArgumentException if both parameters are set.
   */
  protected AbstractFSBuilderImpl(
      @Nonnull Optional<Path> optionalPath) {
    this.optionalPath = optionalPath;
  }

  protected AbstractFSBuilderImpl(@Nonnull final Path path) {
    this(Optional.of(path));
  }

  /**
   * Get the cast builder.
   *
   * @return this object, typecast
   */
  public B getThisBuilder() {
    return (B) this;
  }

  /**
   * Get the optional path; may be empty.
   *
   * @return the optional path field.
   */
  public Optional<Path> getOptionalPath() {
    return optionalPath;
  }

  /**
   * Get the path: only valid if constructed with a path.
   *
   * @return the path
   *
   * @throws NoSuchElementException if the field is empty.
   */
  public Path getPath() {
    return optionalPath.get();
  }

  /**
   * Set optional Builder parameter.
   */
  @Override
  public B opt(@Nonnull final String key, @Nonnull final String value) {
    mandatoryKeys.remove(key);
    optionalKeys.add(key);
    options.set(key, value);
    return getThisBuilder();
  }

  /**
   * Set optional boolean parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  @Override
  public B opt(@Nonnull final String key, boolean value) {
    return opt(key, Boolean.toString(value));
  }

  /**
   * Set optional int parameter for the Builder.
   *
   * @see #opt(String, String)
   */
  @Override
  public B opt(@Nonnull final String key, int value) {
    return opt(key, Integer.toString(value));
  }

  @Override
  public B optLong(@Nonnull final String key, final long value) {
    return opt(key, Long.toString(value));
  }

  /**
   * Set mandatory option to the Builder.
   *
   * If the option is not supported or unavailable on the {@link FileSystem},
   * the client should expect {@link #build()} throws IllegalArgumentException.
   */
  @Override
  public B must(@Nonnull final String key, @Nonnull final String value) {
    mandatoryKeys.add(key);
    options.set(key, value);
    return getThisBuilder();
  }

  /**
   * Set mandatory boolean option.
   *
   * @see #must(String, String)
   */
  @Override
  public B must(@Nonnull final String key, boolean value) {
    return must(key, Boolean.toString(value));
  }

  /**
   * Set mandatory int option.
   *
   * @see #must(String, String)
   */
  @Override
  public B must(@Nonnull final String key, int value) {
    return must(key, Integer.toString(value));
  }

  @Override
  public B mustLong(@Nonnull final String key, final long value) {
    return must(key, Long.toString(value));
  }

  /**
   * Get the mutable option configuration.
   *
   * @return the option configuration.
   */
  public Configuration getOptions() {
    return options;
  }

  /**
   * Get all the keys that are set as mandatory keys.
   *
   * @return mandatory keys.
   */
  public Set<String> getMandatoryKeys() {
    return Collections.unmodifiableSet(mandatoryKeys);
  }

  /**
   * Get all the keys that are set as optional keys.
   *
   * @return optional keys.
   */
  public Set<String> getOptionalKeys() {
    return Collections.unmodifiableSet(optionalKeys);
  }

  /**
   * Reject a configuration if one or more mandatory keys are
   * not in the set of mandatory keys.
   * The first invalid key raises the exception; the order of the
   * scan and hence the specific key raising the exception is undefined.
   *
   * @param knownKeys a possibly empty collection of known keys
   * @param extraErrorText extra error text to include.
   *
   * @throws IllegalArgumentException if any key is unknown.
   */
  protected void rejectUnknownMandatoryKeys(final Collection<String> knownKeys,
      String extraErrorText)
      throws IllegalArgumentException {
    rejectUnknownMandatoryKeys(mandatoryKeys, knownKeys, extraErrorText);
  }

  /**
   * Reject a configuration if one or more mandatory keys are
   * not in the set of mandatory keys.
   * The first invalid key raises the exception; the order of the
   * scan and hence the specific key raising the exception is undefined.
   *
   * @param mandatory the set of mandatory keys
   * @param knownKeys a possibly empty collection of known keys
   * @param extraErrorText extra error text to include.
   *
   * @throws IllegalArgumentException if any key is unknown.
   */
  public static void rejectUnknownMandatoryKeys(
      final Set<String> mandatory,
      final Collection<String> knownKeys,
      final String extraErrorText)
      throws IllegalArgumentException {
    final String eText = extraErrorText.isEmpty()
        ? ""
        : (extraErrorText + " ");
    mandatory.forEach((key) ->
        checkArgument(knownKeys.contains(key),
            UNKNOWN_MANDATORY_KEY + " %s\"%s\"", eText, key));
  }

}
