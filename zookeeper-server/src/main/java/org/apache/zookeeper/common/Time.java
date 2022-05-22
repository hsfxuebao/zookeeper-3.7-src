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

package org.apache.zookeeper.common;

import java.util.Date;

public class Time {

    /**
     * Returns time in milliseconds as does System.currentTimeMillis(),
     * but uses elapsed time from an arbitrary epoch more like System.nanoTime().
     * The difference is that if somebody changes the system clock,
     * Time.currentElapsedTime will change but nanoTime won't. On the other hand,
     * all of ZK assumes that time is measured in milliseconds.
     * @return The time in milliseconds from some arbitrary point in time.
     *
     * 和System.currentTimeMillis()一样，它以毫秒为单位返回时间，但是使用从任意纪元（更像System.nanoTime()）经过的时间
     * 不同的是，如果有人改变了系统时钟，当前时间会改变，但纳秒时间不会，另一方面，所有zk都假设时间是以毫秒为单位的
     */
    public static long currentElapsedTime() {
        // 1秒=10^9纳秒
        return System.nanoTime() / 1000000;
    }

    /**
     * Explicitly returns system dependent current wall time.
     * @return Current time in msec.
     */
    public static long currentWallTime() {
        return System.currentTimeMillis();
    }

    /**
     * This is to convert the elapsedTime to a Date.
     * @return A date object indicated by the elapsedTime.
     */
    public static Date elapsedTimeToDate(long elapsedTime) {
        long wallTime = currentWallTime() + elapsedTime - currentElapsedTime();
        return new Date(wallTime);
    }

}
