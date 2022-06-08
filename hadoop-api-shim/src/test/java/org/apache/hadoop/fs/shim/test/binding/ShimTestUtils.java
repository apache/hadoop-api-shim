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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test utils, including stuff copied from ContractTestUtils.
 */
public final class ShimTestUtils {

  private ShimTestUtils() {
  }

  /**
   * Function which calls {@code InputStream.read()} and
   * downgrades an IOE to a runtime exception.
   * @param in input
   * @return the read value
   * @throws AssertionError on any IOException
   */
  public static int read(InputStream in) {
    try {
      return in.read();
    } catch (IOException ex) {
      throw new AssertionError(ex);
    }
  }

  /**
   * Read a whole stream; downgrades an IOE to a runtime exception.
   * @param in input
   * @return the number of bytes read.
   * @throws AssertionError on any IOException
   */
  public static long readStream(InputStream in) {
    long count = 0;

    while (read(in) >= 0) {
      count++;
    }
    return count;
  }


  /**
   * Expect a future to raise a specific exception class when evaluated,
   * <i>looking inside the raised {@code ExecutionException}</i> for it.
   * @param clazz class of exception; the nested exception must be this class
   * <i>or a subclass</i>.
   *
   * This is simply an unwrapping of the outcome of the future.
   *
   * If an exception is not raised, the return value of the {@code get()}
   * call is included in the exception string.
   *
   * If the nested cause of the raised ExecutionException is not an
   * Exception (i.e its an error), then the outer ExecutionException is
   * rethrown.
   * This keeps the operation signatures in sync.
   *
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param future future to get
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type and contents
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException if the raised exception didn't contain an
   * exception.
   * @throws InterruptedException if the current thread was interrupted
   * @throws TimeoutException if the wait timed out
   * @throws Exception if the wrong exception was raised, or there was
   * a text mismatch.
   */
  public static <T, E extends Throwable> E interceptFuture(
      Class<E> clazz,
      String contained,
      Future<T> future) throws Exception {
    return intercept(clazz,
        contained,
        () -> {
          try {
            return future.get();
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
              throw (Exception) cause;
            } else {
              throw e;
            }
          }
        });
  }

  /**
   * Expect a future to raise a specific exception class when evaluated,
   * <i>looking inside the raised {@code ExecutionException}</i> for it.
   * @param clazz class of exception; the nested exception must be this class
   * <i>or a subclass</i>.
   *
   * This is simply an unwrapping of the outcome of the future.
   *
   * If an exception is not raised, the return value of the {@code get()}
   * call is included in the exception string.
   *
   * If the nested cause of the raised ExecutionException is not an
   * Exception (i.e its an error), then the outer ExecutionException is
   * rethrown.
   * This keeps the operation signatures in sync.
   *
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param future future to get
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type and contents
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException if the raised exception didn't contain an
   * exception.
   * @throws InterruptedException if the current thread was interrupted
   * @throws TimeoutException if the wait timed out
   * @throws Exception if the wrong exception was raised, or there was
   * a text mismatch.
   */
  public static <T, E extends Throwable> E interceptFuture(
      final Class<E> clazz,
      final String contained,
      final long timeout,
      final TimeUnit tu,
      final Future<T> future) throws Exception {
    return intercept(clazz,
        contained,
        () -> {
          try {
            return future.get(timeout, tu);
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
              throw (Exception) cause;
            } else {
              throw e;
            }
          }
        });
   }

}
