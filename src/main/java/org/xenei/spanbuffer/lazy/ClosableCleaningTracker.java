/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.spanbuffer.lazy;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Based on the FileCleaningTracker.java from Apache commons-io.
 * <p>
 * Keeps track of closeables awaiting closure, and closes them when an associated
 * marker object is reclaimed by the garbage collector.</p>
 * <p>
 * This utility creates a background thread to handle closures.
 * Each object to be closed is registered with a handler object.
 * When the handler object is garbage collected, the object is closed.
 * <p>
 * In an environment with multiple class loaders (a servlet container, for
 * example), you should consider stopping the background thread if it is no
 * longer needed. This is done by invoking the method
 * {@link #exitWhenFinished}, typically in
 * {@code javax.servlet.ServletContextListener.contextDestroyed(javax.servlet.ServletContextEvent)} or similar.
 *
 */
public class ClosableCleaningTracker {

    // Note: fields are package protected to allow use by test cases

    /**
     * Queue of <code>Tracker</code> instances being watched.
     */
    ReferenceQueue<Object> q = new ReferenceQueue<>();
    /**
     * Collection of <code>Tracker</code> instances in existence.
     */
    final Collection<Tracker> trackers = Collections.synchronizedSet(new HashSet<Tracker>()); // synchronized
    /**
     * Collection of Closable paths that failed to close.
     */
    final List<Closeable> closeFailures = Collections.synchronizedList(new ArrayList<Closeable>());
    /**
     * Whether to terminate the thread when the tracking is complete.
     */
    volatile boolean exitWhenFinished = false;
    /**
     * The thread that will clean up registered files.
     */
    Thread reaper;

    //-----------------------------------------------------------------------
    /**
     * Track the specified file, using the provided marker, deleting the file
     * when the marker instance is garbage collected.
     * The {@link FileDeleteStrategy#NORMAL normal} deletion strategy will be used.
     *
     * @param closable  the Closable to be tracked, not null
     * @param marker  the marker object used to track the closeable, not null
     * @throws NullPointerException if the closeable is null
     */
    public void track(final Closeable closeable, final Object marker) {
    	addTracker(closeable, marker);
    }

    /**
     * Adds a tracker to the list of trackers.
     *
     * @param closable  the Closable to be tracked, not null
     * @param marker  the marker object used to track the closable, not null
     * @param deleteStrategy  the strategy to delete the file, null means normal
     */
    private synchronized void addTracker(final Closeable closable, final Object marker) {
        // synchronized block protects reaper
        if (exitWhenFinished) {
            throw new IllegalStateException("No new trackers can be added once exitWhenFinished() is called");
        }
        if (reaper == null) {
            reaper = new Reaper();
            reaper.start();
        }
        trackers.add(new Tracker(closable, marker, q));
    }

    //-----------------------------------------------------------------------
    /**
     * Retrieve the number of closeable objects currently being tracked, and therefore
     * awaiting closure.
     *
     * @return the number of closeables being tracked
     */
    public int getTrackCount() {
        return trackers.size();
    }

    /**
     * Return the closeables that failed to close.
     *
     * @return the closeables that failed to close
     * @since 2.0
     */
    public List<Closeable> getCloseFailures() {
        return closeFailures;
    }

    /**
     * Call this method to cause the closable cleaner thread to terminate when
     * there are no more objects being tracked for closure.
     * <p>
     * In a simple environment, you don't need this method as the file cleaner
     * thread will simply exit when the JVM exits. In a more complex environment,
     * with multiple class loaders (such as an application server), you should be
     * aware that the closable cleaner thread will continue running even if the class
     * loader it was started from terminates. This can constitute a memory leak.
     * <p>
     * For example, suppose that you have developed a web application, which
     * contains the commons-io jar file in your WEB-INF/lib directory. In other
     * words, the ClosableCleaner class is loaded through the class loader of your
     * web application. If the web application is terminated, but the servlet
     * container is still running, then the file cleaner thread will still exist,
     * posing a memory leak.
     * <p>
     * This method allows the thread to be terminated. Simply call this method
     * in the resource cleanup code, such as
     * {@code javax.servlet.ServletContextListener.contextDestroyed(javax.servlet.ServletContextEvent)}.
     * Once called, no new objects can be tracked by the file cleaner.
     */
    public synchronized void exitWhenFinished() {
        // synchronized block protects reaper
        exitWhenFinished = true;
        if (reaper != null) {
            synchronized (reaper) {
                reaper.interrupt();
            }
        }
    }

    //-----------------------------------------------------------------------
    /**
     * The reaper thread.
     */
    private final class Reaper extends Thread {
        /** Construct a new Reaper */
        Reaper() {
            super("Closable Reaper");
            setPriority(Thread.MAX_PRIORITY);
            setDaemon(true);
        }

        /**
         * Run the reaper thread that will close objects as their associated
         * marker objects are reclaimed by the garbage collector.
         */
        @Override
        public void run() {
            // thread exits when exitWhenFinished is true and there are no more tracked objects
            while (exitWhenFinished == false || trackers.size() > 0) {
                try {
                    // Wait for a tracker to remove.
                    final Tracker tracker = (Tracker) q.remove(); // cannot return null
                    trackers.remove(tracker);
                    try {
                    	tracker.close();
                    } catch (IOException e) {
                        closeFailures.add(tracker.closeable);
                    } finally {
                    	tracker.clear();
                    }
                } catch (final InterruptedException e) {
                    continue;
                }
            }
        }
    }

    //-----------------------------------------------------------------------
    /**
     * Inner class which acts as the reference for a closeable pending closure.
     */
    private static final class Tracker extends PhantomReference<Object> {

        /**
         * The closable object being tracked.
         */
        private final Closeable closeable;
        

        /**
         * Constructs an instance of this class from the supplied parameters.
         *
         * @param closeable  the closable object to be tracked, not null
         * @param marker  the marker object used to track the object, not null
         * @param queue  the queue on to which the tracker will be pushed, not null
         */
        Tracker(final Closeable closeable, final Object marker,
                final ReferenceQueue<? super Object> queue) {
            super(marker, queue);
            this.closeable = closeable;            
        }


        /**
         * Closes the closable associated with this tracker instance.
         *
         * @throws IOException on error
         */
        public void close() throws IOException {
            closeable.close();
        }
    }

}
