/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;

/**
 * An asynchronous queue that limits the rate at which batches will be made available as well as the number
 * of elements they will contain.
 *
 * @param <T>           The type of elements accepted by the queue.
 */
@ThreadSafe
public class ThrottledAsyncQueue<T>
        extends AsyncQueue<T>
{
    private static final Logger log = Logger.get(ThrottledAsyncQueue.class);

    private final int maxBatchSizePerSec;
    private final Executor executor;
    private final ExecutorService throttleService;

    private ListenableFuture<Bucket> throttleSignal;

    public ThrottledAsyncQueue(int maxBatchSizePerSec, int targetQueueSize, Executor executor)
    {
        super(targetQueueSize, executor);
        this.executor = executor;
        this.throttleService = Executors.newSingleThreadExecutor();
        this.maxBatchSizePerSec = maxBatchSizePerSec;
        this.throttleSignal = immediateFuture(new Bucket(System.currentTimeMillis(), maxBatchSizePerSec));
    }

    @Override
    public synchronized <O> ListenableFuture<O> borrowBatchAsync(int maxSize, Function<List<T>, BorrowResult<T, O>> function)
    {
        /*
        We throttle the dequeue operation to make sure we don't make return more than `maxBatchSizePerSec` elements per
        second.
        There are a few things to consider here:
        1.  The batches are asynchronous: if requesting a batch at time "t", the batch can be immediately returned if
            there are elements in the queue. However if the queue is empty, the batch will only be computed after the
            queue is not empty any more. Thus we can not query the size of the queue synchronously here, we need to wait
            for the signal that the queue is not empty, which is the completion of the future returned by the previous
            batch.
        2.  The `maxSize` parameter is only an upper bound but the batches may be smaller if the queue contains less
            elements (in which case the size of the batch is the size of the queue).
        3.  In order to be able to properly test this method, we need to make sure that if we wait for the result of a
            batch, the next call to this method will return an immediate future if no throttling is required.
            This allows to test by making sure that non-throttled calles to this method return a future for
            which `isDone()` returns true.

        The throttling mechanism is the following with maxBatchSizePerSec = 10:
                                   t                          t+1s                    t+2s
                                   |                           |                       |
        batch future:              | batch   batch  batch      |   batch      batch    |
                                   X-------->X----->X-->X      X---------->X---------->X
                                   |                   throttle|                       |
        throttle future:           X         X      X   X----->X           X           X
                                   |                           |                       |

        batch size:                |   4        4     2        |    5           4      |
        remain. before throttle:   | 10-4=6   6-4=2  0:throttle|    5           1      |
         */

        /*
        This future completes when:
        1.  The previous batch future has completed (i.e. the notEmptySignal signal of the queue)
        2.  Immediately if we polled less than `maxBatchSizePerSec` elements in the last second
            - OR -
            If during the last second we polled `maxBatchSizePerSec`, after a full second has elapsed, at which time
            we can reset the counter for the new second that starts.

        A bucket represents a throttling period (second). During a period, we can only poll at most `maxBatchSizePerSec`
        elements, after which we need to create a new bucket for the next period (second) and wait for the first period
        to complete.
         */
        final ListenableFuture<Bucket> throttleFuture = thenAsync(
                throttleSignal,
                bucket -> bucket.next(),
                throttleService);

        /*
        Once the future above completes, we can then query for a new batch.

        The size of the batch is the minimum between:
        1.  The number of elements that can be still polled during this second before throttling.
        2.  The size of the queue before polling.
        3.  The `maxSize` parameter passed by the caller.
         */
        final ListenableFuture<O> batchFuture = thenAsync(
                throttleFuture,
                bucket -> super.borrowBatchAsync(bucket.atMost(maxSize), function),
                executor);

        /*
        The `throttleSignal` future will complete once this batch is polled, and will return the bucket returned by the
        first future above.

        There are 2 cases:
        1.  The queue was not empty, then the batch is returned immediately as an immediate future
        2.  The queue was empty, the future will complete only when the queue becomes non-empty and there are elements
            to return in the batch.

        The `throttleSignal` has necessarily completed already, we only "join" on it to access its result.
         */
        throttleSignal = onBothComplete(batchFuture, throttleFuture);

        // make sure the executor service is closed after the last batch
        onComplete(batchFuture,
                () -> {
                    if (isFinished()) {
                        throttleService.shutdownNow();
                    }
                });

        return onBothComplete(throttleSignal, batchFuture);
    }

    // chain an async operation after a future, run synchronously if the future is succeeded.
    private static <T, V> ListenableFuture<V> thenAsync(ListenableFuture<T> future,
                                                        Function<T, ListenableFuture<V>> function,
                                                        Executor executor)
    {
        if (future.isDone()) {
            return function.apply(getFutureValue(future));
        }
        return Futures.transformAsync(future, function::apply, executor);
    }

    // execute code after a future completes, or immediately if the future is completed
    private static <T, V> ListenableFuture<V> onComplete(ListenableFuture<T> future, Runnable function)
    {
        if (future.isDone()) {
            function.run();
        }
        return Futures.whenAllComplete(future).call(
                () -> {
                    function.run();
                    return null;
                },
                directExecutor());
    }

    // return the result of the second future once both futures have completed, synchronously
    // if both are already completed.
    private static <V> ListenableFuture<V> onBothComplete(ListenableFuture<?> future1,
                                                          ListenableFuture<V> future2)
    {
        if (future1.isDone() && future2.isDone()) {
            return immediateFuture(getFutureValue(future2));
        }
        return Futures.whenAllComplete(future1, future2).call(
                () -> getFutureValue(future2),
                directExecutor());
    }

    // a Bucket is used to count the number of elements that can still be dequeued in the ongoing second before
    // the polling of the queue needs to be throttled.
    private class Bucket
    {
        private final long bucketMs;
        private final AtomicInteger remainingSize;

        private Bucket(long bucketMs, int remainingSize)
        {
            this.bucketMs = bucketMs;
            this.remainingSize = new AtomicInteger(remainingSize);
        }

        private int atMost(int maxSize)
        {
            // update the remaining bucket size atomically
            int max = Math.min(size(), maxSize);
            int size = Math.min(max, remainingSize.getAndUpdate(
                    v -> v - Math.min(max, v)));
            return size;
        }

        private ListenableFuture<Bucket> next()
        {
            final long nextBucketMs = bucketMs + 1000L;
            final long now = System.currentTimeMillis();

            if (remainingSize.get() > 0) {
                // there are still elements to dequeue in this bucket, do not throttle
                return immediateFuture(this);
            }
            else if (nextBucketMs - now <= 0) {
                // there are not elements left to dequeue but more than 1 sec has elasped, we create a new bucket with
                // rest counters.
                return immediateFuture(new Bucket(now, maxBatchSizePerSec));
            }
            else {
                // wait until a second has elapsed since the creation of the last bucket
                return Futures.transform(
                        throttleSignal,
                        prevBucket -> {
                            try {
                                log.info("throttling " + (nextBucketMs - now));
                                Thread.sleep(Math.max(0L, nextBucketMs - now));
                            }
                            catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            log.info("throttled " + (nextBucketMs - now));
                            return new Bucket(nextBucketMs, maxBatchSizePerSec);
                        },
                        throttleService);
            }
        }
    }
}
