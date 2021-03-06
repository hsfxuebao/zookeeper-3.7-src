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
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl extends ZooKeeperCriticalThread implements SessionTracker {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    // 保存sessionId和对应的Session对象
    protected final ConcurrentHashMap<Long, SessionImpl> sessionsById = new ConcurrentHashMap<Long, SessionImpl>();

    private final ExpiryQueue<SessionImpl> sessionExpiryQueue;

    // key为sessionId，value为这个session的过期时间
    protected final ConcurrentMap<Long, Integer> sessionsWithTimeout;
    // 下一次新建session时的id
    private final AtomicLong nextSessionId = new AtomicLong();

    public static class SessionImpl implements Session {

        SessionImpl(long sessionId, int timeout) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            isClosing = false;
        }

        final long sessionId;
        final int timeout;
        boolean isClosing;

        Object owner;

        public long getSessionId() {
            return sessionId;
        }
        public int getTimeout() {
            return timeout;
        }
        public boolean isClosing() {
            return isClosing;
        }

        public String toString() {
            return "0x" + Long.toHexString(sessionId);
        }

    }

    /**
     * Generates an initial sessionId.
     *
     * <p>High order 1 byte is serverId, next 5 bytes are from timestamp, and low order 2 bytes are 0s.
     * Use {@literal ">>> 8"}, not {@literal ">> 8"} to make sure that the high order 1 byte is entirely
     * up to the server Id.
     *
     * <p>See also http://jira.apache.org/jira/browse/ZOOKEEPER-1622
     *
     * @param id server Id
     * @return the session Id
     */
    public static long initializeNextSessionId(long id) {
        long nextSid;
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        nextSid = nextSid | (id << 56);
        if (nextSid == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
            ++nextSid;  // this is an unlikely edge case, but check it just in case
        }
        return nextSid;
    }

    private final SessionExpirer expirer;

    //SessionTrackerImpl调用的构造
    public SessionTrackerImpl(SessionExpirer expirer, ConcurrentMap<Long, Integer> sessionsWithTimeout, int tickTime, long serverId, ZooKeeperServerListener listener) {
        super("SessionTracker", listener);
        this.expirer = expirer;
        // 创建并初始化会话过期队列
        this.sessionExpiryQueue = new ExpiryQueue<SessionImpl>(tickTime);
        this.sessionsWithTimeout = sessionsWithTimeout;
        this.nextSessionId.set(initializeNextSessionId(serverId));
        //从内存中取出之前保存的会话数据，重新加载到sessionTracker（应该是选举后重启之类的场景）
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            // sessionsWithTimeout是一个map，key为sessionId，value为该会话对应的timeout
            trackSession(e.getKey(), e.getValue());
        }

        EphemeralType.validateServerId(serverId);
    }

    volatile boolean running = true;

    public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session ");
        sessionExpiryQueue.dump(pwriter);
    }

    /**
     * Returns a mapping from time to session IDs of sessions expiring at that time.
     */
    public synchronized Map<Long, Set<Long>> getSessionExpiryMap() {
        // Convert time -> sessions map to time -> session IDs map
        Map<Long, Set<SessionImpl>> expiryMap = sessionExpiryQueue.getExpiryMap();
        Map<Long, Set<Long>> sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        for (Entry<Long, Set<SessionImpl>> e : expiryMap.entrySet()) {
            Set<Long> ids = new HashSet<Long>();
            sessionExpiryMap.put(e.getKey(), ids);
            for (SessionImpl s : e.getValue()) {
                ids.add(s.sessionId);
            }
        }
        return sessionExpiryMap;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    // 该run()方法就是用于对过期会话及会话桶进行清理的，
    // 而这个清理工作是定时进行的。但这个定时功能不是通过
    // 定时器完成的，而是通过等待完成的。
    @Override
    public void run() {
        try {
            // 这个方法很简单，只需要每隔一个expirationInterval时间便从待删除
            // Session集合sessionSets中取出Session进行过期操作就行
            // 当然，这里很简单就说明在另外一个地方进行了SessionTimeout更新操作
            // 上一篇源码便介绍过本类中的touchSession，因此有兴趣的去翻看上一篇
            // 分析SessionTrackerImpl的源码解析，结合起来分析便可以知道ZK的巧妙
            while (running) {
                // 判断一下当前时间是否已经到达了下次过期时间点
                long waitTime = sessionExpiryQueue.getWaitTime();
                if (waitTime > 0) {
                    // 如果未到则直接阻塞剩余等待
                    Thread.sleep(waitTime);
                    continue;
                }

                // 将nextExpirationTime时间点将要过期的Session全部取出来
                // sessionExpiryQueue.poll() 用于清理当前过期的会话桶
                // for()用于遍历这个会话桶中的所有会话
                for (SessionImpl s : sessionExpiryQueue.poll()) {
                    ServerMetrics.getMetrics().STALE_SESSIONS_EXPIRED.add(1);
                    // 将这些Session逐个关闭并进行过期操作
                    setSessionClosing(s.sessionId);
                    // 这里面的过期操作实际上就是向客户端发送一个
                    // closeSession类型的响应
                    expirer.expire(s);
                }
            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    public synchronized boolean touchSession(long sessionId, int timeout) {
        SessionImpl s = sessionsById.get(sessionId);

        if (s == null) {
            logTraceTouchInvalidSession(sessionId, timeout);
            return false;
        }

        if (s.isClosing()) {
            logTraceTouchClosingSession(sessionId, timeout);
            return false;
        }

        updateSessionExpiry(s, timeout);
        return true;
    }

    private void updateSessionExpiry(SessionImpl s, int timeout) {
        logTraceTouchSession(s.sessionId, timeout, "");
        // 更新过期时间
        sessionExpiryQueue.update(s, timeout);
    }

    private void logTraceTouchSession(long sessionId, int timeout, String sessionStatus) {
        if (LOG.isTraceEnabled()) {
            String msg = MessageFormat.format(
                "SessionTrackerImpl --- Touch {0}session: 0x{1} with timeout {2}",
                sessionStatus,
                Long.toHexString(sessionId),
                Integer.toString(timeout));

            ZooTrace.logTraceMessage(LOG, ZooTrace.CLIENT_PING_TRACE_MASK, msg);
        }
    }

    private void logTraceTouchInvalidSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "invalid ");
    }

    private void logTraceTouchClosingSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "closing ");
    }

    public int getSessionTimeout(long sessionId) {
        return sessionsWithTimeout.get(sessionId);
    }

    public synchronized void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Session closing: 0x{}", Long.toHexString(sessionId));
        }

        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        s.isClosing = true;
    }

    public synchronized void removeSession(long sessionId) {
        LOG.debug("Removing session 0x{}", Long.toHexString(sessionId));
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "SessionTrackerImpl --- Removing session 0x" + Long.toHexString(sessionId));
        }
        if (s != null) {
            sessionExpiryQueue.remove(s);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(), "Shutdown SessionTrackerImpl!");
        }
    }

    public long createSession(int sessionTimeout) {
        long sessionId = nextSessionId.getAndIncrement();
        // 在使用RequestProcessor处理请求前会调用该方法为客户端创建一个session
        trackSession(sessionId, sessionTimeout);
        return sessionId;
    }

    @Override
    public synchronized boolean trackSession(long id, int sessionTimeout) {
        boolean added = false;

        // 如果没有保存对应的Session对象则创建一个并添加
        SessionImpl session = sessionsById.get(id);
        if (session == null) {
            session = new SessionImpl(id, sessionTimeout);
        }

        // findbugs2.0.3 complains about get after put.
        // long term strategy would be use computeIfAbsent after JDK 1.8
        SessionImpl existedSession = sessionsById.putIfAbsent(id, session);

        if (existedSession != null) {
            session = existedSession;
        } else {
            added = true;
            LOG.debug("Adding session 0x{}", Long.toHexString(id));
        }

        if (LOG.isTraceEnabled()) {
            String actionStr = added ? "Adding" : "Existing";
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "SessionTrackerImpl --- " + actionStr
                + " session 0x" + Long.toHexString(id) + " " + sessionTimeout);
        }
        // 添加完session后更新session的过期时间
        updateSessionExpiry(session, sessionTimeout);
        return added;
    }

    public synchronized boolean commitSession(long id, int sessionTimeout) {
        return sessionsWithTimeout.put(id, sessionTimeout) == null;
    }

    public boolean isTrackingSession(long sessionId) {
        return sessionsById.containsKey(sessionId);
    }

    public synchronized void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException, KeeperException.UnknownSessionException {
        LOG.debug("Checking session 0x{}", Long.toHexString(sessionId));
        SessionImpl session = sessionsById.get(sessionId);

        if (session == null) {
            throw new KeeperException.UnknownSessionException();
        }

        if (session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }

        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    public synchronized void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }

    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        try {
            checkSession(sessionId, owner);
        } catch (KeeperException.UnknownSessionException e) {
            throw new KeeperException.SessionExpiredException();
        }
    }

    public long getLocalSessionCount() {
        return 0;
    }

    @Override
    public boolean isLocalSessionsEnabled() {
        return false;
    }

    public Set<Long> globalSessions() {
        return sessionsById.keySet();
    }

    public Set<Long> localSessions() {
        return Collections.emptySet();
    }
}
