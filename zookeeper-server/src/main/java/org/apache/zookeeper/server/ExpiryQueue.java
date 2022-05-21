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

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 */
public class ExpiryQueue<E> {

    // elemMap集合的key为session，value为该session的过期时间，
    // 即该session当前所在的会话桶id
    private final ConcurrentHashMap<E, Long> elemMap = new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap = new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong nextExpirationTime = new AtomicLong();
    private final int expirationInterval;

    public ExpiryQueue(int expirationInterval) {
        // 会话桶大小
        this.expirationInterval = expirationInterval;
        // roundToNextInterval(Time.currentElapsedTime()) 计算当前时间所在的会话桶，
        // 每个会话桶都有一个标识id，即其所包含的时间范围的最大边界时间点
        // nextExpirationTime 用于记录下次要进行过期会话清理的时间点
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    // 计算指定时间所在的会话桶
    private long roundToNextInterval(long time) {
        // 利用整型除整型结果仍为整型来计算会话桶的
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     * @param elem  element to remove
     * @return time at which the element was set to expire, or null if
     *              it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     * @param elem     element to add/update
     * @param timeout  timout in milliseconds
     * @return time at which the element is now set to expire if
     *                 changed, or null if unchanged
     */
    // 当client与server有交互时（连接请求/读写操作/心跳），该方法就会被调用
    // 当zk server启动时会将磁盘中的session恢复到内存，也会调用该方法
    // 该方法在做的是会话换桶操作
    public Long update(E elem, int timeout) {
        // elemMap集合的key为session，value为该session的过期时间，
        // 即该session当前所在的会话桶id
        Long prevExpiryTime = elemMap.get(elem);
        long now = Time.currentElapsedTime();
        // 计算本次交互应该将会话放入到哪个会话桶
        Long newExpiryTime = roundToNextInterval(now + timeout);

        // 若之前所在会话桶id与本次交互计算的会话桶id相同，
        // 则无需换桶，即什么也不用做
        if (newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        // ---------- 代码能走到这里，说明需要换桶了。 --------------
        // 换桶由两步操作完成：将会话放入到新桶；将会话从老桶中清除

        // First add the elem to the new expiry time bucket in expiryMap.
        // 从会话桶集合中获取当前的会话桶，若为null，则创建一个新的会话桶
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            // 创建会话桶set
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            // 将新建的会话桶放入到会话桶集合
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        // 将会话放入到会话桶
        set.add(elem);

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        // 将会话与会话桶id的对应关系放入到elemMap，并获取到该会话之前所在的会话桶id
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        // 若当前会话桶id与之前会话桶id不相同，说明需要换桶。
        // 而前面已经将会话放到了新的会话桶，所以这里要将会话从老桶中清除
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            // 获取到之前的会话桶
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                // 将会话从老会话桶中清除
                prevSet.remove(elem);
            }
        }
        // 返回当前交互引发的会话所在的会话桶id，
        // 即当前会话的真正过期时间点
        return newExpiryTime;
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        // 获取本次要进行过期清理的时间点 nextExpirationTime是此时还在使用的那个桶的边界时间
        // 刚启动的时候会为nextExpirationTime赋值
        long expirationTime = nextExpirationTime.get();
        // 计算当前时间距离清理时间点还有多久
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     *         ready
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        // 若当前时间小于清理时间点，说明还没有到清理时间，直接返回空集合，无需清理
        if (now < expirationTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;
        // 计算下次清理时间点
        long newExpirationTime = expirationTime + expirationInterval;
        // 通过CAS更新清理时间点
        if (nextExpirationTime.compareAndSet(expirationTime, newExpirationTime)) {
            // 将当前清理时间点作为会话桶id的会话桶从会话桶集合中remove掉
            set = expiryMap.remove(expirationTime);
        }
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -&gt; elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }

}

