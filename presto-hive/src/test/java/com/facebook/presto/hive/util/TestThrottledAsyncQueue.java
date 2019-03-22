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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.Threads;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestThrottledAsyncQueue
{
    private ExecutorService executor;

    @BeforeClass
    public void setUpClass()
    {
        executor = Executors.newFixedThreadPool(8, Threads.daemonThreadsNamed("test-async-queue-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        executor.shutdownNow();
    }

    @Test
    public void testThrottle()
    {
        // Make sure that the dequeuing is throttled even if we have enough elements in the queue

        ThrottledAsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(3, 10, executor);
        assertTrue(queue.offer(1).isDone());
        assertTrue(queue.offer(2).isDone());
        assertTrue(queue.offer(3).isDone());
        assertTrue(queue.offer(4).isDone());
        assertTrue(queue.offer(5).isDone());
        assertTrue(queue.offer(6).isDone());
        queue.finish();

        // no throttling, enough elements in the queue
        ListenableFuture<List<Integer>> future1 = queue.getBatchAsync(2);
        assertTrue(future1.isDone());
        assertEquals(getFutureValue(future1), ImmutableList.of(1, 2));
        assertFalse(queue.isFinished());

        // we can only dequeue one more element before being throttled
        ListenableFuture<List<Integer>> future2 = queue.getBatchAsync(2);
        assertTrue(future2.isDone());
        assertEquals(getFutureValue(future2), ImmutableList.of(3));
        assertFalse(queue.isFinished());

        // we are now throttled, this future will not be immediate
        ListenableFuture<List<Integer>> future3 = queue.getBatchAsync(2);
        assertFalse(future3.isDone());
        assertEquals(getFutureValue(future3), ImmutableList.of(4, 5));
        assertFalse(queue.isFinished());

        // not throttled anymore as we can still dequeue 1 element
        ListenableFuture<List<Integer>> future4 = queue.getBatchAsync(2);
        assertTrue(future4.isDone());
        assertEquals(getFutureValue(future4), ImmutableList.of(6));
        assertTrue(queue.isFinished());
    }

    @Test
    public void testBorrowThrows()
            throws Exception
    {
        // It doesn't matter the exact behavior when the caller-supplied function to borrow fails.
        // However, it must not block pending futures.

        AsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(100, 4, executor);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);
        queue.offer(5);

        ListenableFuture<?> future1 = queue.offer(6);
        assertFalse(future1.isDone());

        Runnable runnable = () -> {
            getFutureValue(queue.borrowBatchAsync(1, elements -> {
                throw new RuntimeException("test fail");
            }));
        };

        try {
            executor.submit(runnable).get();
            fail("expected failure");
        }
        catch (ExecutionException e) {
            assertContains(e.getMessage(), "test fail");
        }

        ListenableFuture<?> future2 = queue.offer(7);
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        queue.finish();
        future1.get();
        future2.get();
        assertTrue(queue.offer(8).isDone());

        try {
            executor.submit(runnable).get();
            fail("expected failure");
        }
        catch (ExecutionException e) {
            assertContains(e.getMessage(), "test fail");
        }

        assertTrue(queue.offer(9).isDone());

        assertFalse(queue.isFinished());
        ArrayList<Integer> list = new ArrayList<>(queue.getBatchAsync(100).get());
        // 1 and 2 were removed by borrow call; 8 and 9 were never inserted because insertion happened after finish.
        assertEquals(list, ImmutableList.of(3, 4, 5, 6, 7));
        assertTrue(queue.isFinished());
    }
}
