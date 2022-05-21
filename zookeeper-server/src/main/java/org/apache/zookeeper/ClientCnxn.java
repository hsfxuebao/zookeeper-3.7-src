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

package org.apache.zookeeper;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.AllChildrenNumberCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.EphemeralsCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetAllChildrenNumberResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetEphemeralsResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SetWatches2;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 *
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ClientCnxn {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxn.class);

    /* ZOOKEEPER-706: If a session has a large number of watches set then
     * attempting to re-establish those watches after a connection loss may
     * fail due to the SetWatches request exceeding the server's configured
     * jute.maxBuffer value. To avoid this we instead split the watch
     * re-establishement across multiple SetWatches calls. This constant
     * controls the size of each call. It is set to 128kB to be conservative
     * with respect to the server's 1MB default for jute.maxBuffer.
     */
    private static final int SET_WATCHES_MAX_LENGTH = 128 * 1024;

    /* predefined xid's values recognized as special by the server */
    // -1 means notification(WATCHER_EVENT)
    public static final int NOTIFICATION_XID = -1;
    // -2 is the xid for pings
    public static final int PING_XID = -2;
    // -4 is the xid for AuthPacket
    public static final int AUTHPACKET_XID = -4;
    // -8 is the xid for setWatch
    public static final int SET_WATCHES_XID = -8;

    static class AuthData {

        AuthData(String scheme, byte[] data) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte[] data;

    }

    private final CopyOnWriteArraySet<AuthData> authInfo = new CopyOnWriteArraySet<AuthData>();

    /**
     * These are the packets that have been sent and are waiting for a response.
     */
    // 当Client端的数据包Packet被发送出去时，如果不是ping和auth两种操作类型，其
    // 它操作类型的包都会保存在队列末尾，代表着已发送但未完成的数据，在最后Client
    // 端收到ZK的响应时，将会把队列第一个拿出来进行响应的处理。采用的是FIFO模式，
    // 是因为ZK的Server端接收请求处理请求是有序的，处理完前面一个才会处理后面一个
    // 因此客户端可以采用FIFO的模式处理
    private final Queue<Packet> pendingQueue = new ArrayDeque<>();

    /**
     * These are the packets that need to be sent.
     */
    // 发送队列，当Client端有请求需要发送时将会封装成Packet包添加到这里面，在
    // SendThread线程轮询到有数据时将会取出第一个包数据进行处理发送。使用的也是
    // FIFO模式
    private final LinkedBlockingDeque<Packet> outgoingQueue = new LinkedBlockingDeque<Packet>();

    // 连接时间，初始化时等于客户端sessionTimeout / 可用连接串数量，如果连接成功
    // 后将会等于协约时间negotiatedSessionTimeout / 可用连接串数量，因此正常
    // 而言，此值就是negotiatedSessionTimeout / 可用连接串数量
    private int connectTimeout;

    /**
     * The timeout in ms the client negotiated with the server. This is the
     * "real" timeout, not the timeout request by the client (which may have
     * been increased/decreased by the server which applies bounds to this
     * value.
     */
    // 协约时间，ZK的Server端会设置tickTime，Client端会传sessionTimeout，ZK的
    // Server端将会根据两边的配置进行计算得出两边都能接受的时间，然后返回。这个
    // 字段保存的就是协商之后的session过期时间
    private volatile int negotiatedSessionTimeout;
    // 读取过期时间，连接时值为sessionTimeout * 2 / 3，当连接成功后值为
    // negotiatedSessionTimeout * 2 / 3
    private int readTimeout;
    // 开发人员自己定义的客户端过期时间sessionTimeout（注意这个时间并不是最终
    // Client端运行时的心跳检测时间，后续会出一篇这些时间的具体作用以及计算规则）
    private final int sessionTimeout;

    // 客户端的监听器管理类，包含了默认监听器和三种不同类型的监听器
    private final ZKWatchManager watchManager;

    // 本客户端连接实例的sessionId
    private long sessionId;

    private byte[] sessionPasswd;

    /**
     * If true, the connection is allowed to go to r-o mode. This field's value
     * is sent, besides other data, during session creation handshake. If the
     * server on the other side of the wire is partitioned it'll accept
     * read-only clients only.
     */
    // 是否只可读
    private boolean readOnly;
    // 将来将会被删除，暂时不知道有何用
    final String chrootPath;
    // Client端对Server端发送和接收消息的线程对象
    final SendThread sendThread;
    // Client端负责处理响应事件的线程对象
    final EventThread eventThread;

    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     */
    // Client端的连接是否已经关闭
    private volatile boolean closing = false;

    /**
     * A set of ZooKeeper hosts this client could connect to.
     */
    // 连接串的解析后获得的InetSocketAddress提供对象
    private final HostProvider hostProvider;

    /**
     * Is set to true when a connection to a r/w server is established for the
     * first time; never changed afterwards.
     * <p>
     * Is used to handle situations when client without sessionId connects to a
     * read-only server. Such client receives "fake" sessionId from read-only
     * server, but this sessionId is invalid for other servers. So when such
     * client finds a r/w server, it sends 0 instead of fake sessionId during
     * connection handshake and establishes new, valid session.
     * <p>
     * If this field is false (which implies we haven't seen r/w server before)
     * then non-zero sessionId is fake, otherwise it is valid.
     */
    volatile boolean seenRwServerBefore = false;

    private final ZKClientConfig clientConfig;
    /**
     * If any request's response in not received in configured requestTimeout
     * then it is assumed that the response packet is lost.
     */
    private long requestTimeout;

    ZKWatchManager getWatcherManager() {
        return watchManager;
    }

    public long getSessionId() {
        return sessionId;
    }

    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }

    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = sendThread.getClientCnxnSocket().getLocalSocketAddress();
        SocketAddress remote = sendThread.getClientCnxnSocket().getRemoteSocketAddress();
        sb.append("sessionid:0x").append(Long.toHexString(getSessionId()))
          .append(" local:").append(local)
          .append(" remoteserver:").append(remote)
          .append(" lastZxid:").append(lastZxid)
          .append(" xid:").append(xid)
          .append(" sent:").append(sendThread.getClientCnxnSocket().getSentCount())
          .append(" recv:").append(sendThread.getClientCnxnSocket().getRecvCount())
          .append(" queuedpkts:").append(outgoingQueue.size())
          .append(" pendingresp:").append(pendingQueue.size())
          .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }

    /**
     * This class allows us to pass the headers and the relevant records around.
     */
    static class Packet {

        RequestHeader requestHeader;

        ReplyHeader replyHeader;

        Record request;

        Record response;

        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot) **/
        String clientPath;
        /** Servers's view of the path (may differ due to chroot) **/
        String serverPath;

        boolean finished;

        AsyncCallback cb;

        Object ctx;

        WatchRegistration watchRegistration;

        public boolean readOnly;

        WatchDeregistration watchDeregistration;

        /** Convenience ctor */
        Packet(
            RequestHeader requestHeader,
            ReplyHeader replyHeader,
            Record request,
            Record response,
            WatchRegistration watchRegistration) {
            this(requestHeader, replyHeader, request, response, watchRegistration, false);
        }

        Packet(
            RequestHeader requestHeader,
            ReplyHeader replyHeader,
            Record request,
            Record response,
            WatchRegistration watchRegistration,
            boolean readOnly) {

            this.requestHeader = requestHeader;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            this.readOnly = readOnly;
            this.watchRegistration = watchRegistration;
        }

        public void createBB() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len"); // We'll fill this in later
                if (requestHeader != null) {
                    requestHeader.serialize(boa, "header");
                }
                if (request instanceof ConnectRequest) {
                    request.serialize(boa, "connect");
                    // append "am-I-allowed-to-be-readonly" flag
                    boa.writeBool(readOnly, "readOnly");
                } else if (request != null) {
                    request.serialize(boa, "request");
                }
                baos.close();
                this.bb = ByteBuffer.wrap(baos.toByteArray());
                this.bb.putInt(this.bb.capacity() - 4);
                this.bb.rewind();
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + requestHeader);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }

    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider the list of ZooKeeper servers to connect to
     * @param sessionTimeout the timeout for connections.
     * @param clientConfig the client configuration.
     * @param defaultWatcher default watcher for this connection
     * @param clientCnxnSocket the socket implementation used (e.g. NIO/Netty)
     * @param canBeReadOnly whether the connection is allowed to go to read-only mode in case of partitioning
     */
    public ClientCnxn(
        String chrootPath,
        HostProvider hostProvider,
        int sessionTimeout,
        ZKClientConfig clientConfig,
        Watcher defaultWatcher,
        ClientCnxnSocket clientCnxnSocket,
        boolean canBeReadOnly
    ) throws IOException {
        // 没有连接密码的构造函数
        this(
            chrootPath,
            hostProvider,
            sessionTimeout,
            clientConfig,
            defaultWatcher,
            clientCnxnSocket,
            0,
            new byte[16],
            canBeReadOnly);
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider the list of ZooKeeper servers to connect to
     * @param sessionTimeout the timeout for connections.
     * @param clientConfig the client configuration.
     * @param defaultWatcher default watcher for this connection
     * @param clientCnxnSocket the socket implementation used (e.g. NIO/Netty)
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @param canBeReadOnly whether the connection is allowed to go to read-only mode in case of partitioning
     * @throws IOException in cases of broken network
     */
    public ClientCnxn(
        String chrootPath,
        HostProvider hostProvider,
        int sessionTimeout,
        ZKClientConfig clientConfig,
        Watcher defaultWatcher,
        ClientCnxnSocket clientCnxnSocket,
        long sessionId,
        byte[] sessionPasswd,
        boolean canBeReadOnly
    ) throws IOException {
        // 最终调用赋值的构造函数
        this.chrootPath = chrootPath;
        this.hostProvider = hostProvider;
        this.sessionTimeout = sessionTimeout;
        this.clientConfig = clientConfig;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;
        this.readOnly = canBeReadOnly;

        this.watchManager = new ZKWatchManager(
                clientConfig.getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET),
                defaultWatcher);
        // 计算未连接时的过期时间
        this.connectTimeout = sessionTimeout / hostProvider.size();
        this.readTimeout = sessionTimeout * 2 / 3;
        // 初始化两个线程对象
        this.sendThread = new SendThread(clientCnxnSocket);
        this.eventThread = new EventThread();
        initRequestTimeout();
    }

    public void start() {
        // 启动连接线程
        sendThread.start();
        // 启动事件线程
        eventThread.start();
    }

    private Object eventOfDeath = new Object();

    private static class WatcherSetEventPair {

        private final Set<Watcher> watchers;
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }

    }

    /**
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread
     * names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().replaceAll("-EventThread", "");
        return name + suffix;
    }

    /**
     * Tests that current thread is the main event loop.
     * This method is useful only for tests inside ZooKeeper project
     * it is not a public API intended for use by external applications.
     * @return true if Thread.currentThread() is an EventThread.
     */
    public static boolean isInEventThread() {
        return Thread.currentThread() instanceof EventThread;
    }

    class EventThread extends ZooKeeperThread {
        // 将要处理的ZK事件集合
        private final LinkedBlockingQueue<Object> waitingEvents = new LinkedBlockingQueue<Object>();

        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        private volatile boolean wasKilled = false;
        private volatile boolean isRunning = false;

        EventThread() {
            super(makeThreadName("-EventThread"));
            setDaemon(true);
        }

        public void queueEvent(WatchedEvent event) {
            queueEvent(event, null);
        }

        private void queueEvent(WatchedEvent event, Set<Watcher> materializedWatchers) {
            // SendThread就是调用这个方法将对应的ZK事件传入进来开始ZK事件的生命周期
            // 如果session状态和当前一样且事件类型没有则直接退出，无需处理
            if (event.getType() == EventType.None && sessionState == event.getState()) {
                return;
            }
            sessionState = event.getState();
            final Set<Watcher> watchers;
            if (materializedWatchers == null) {
                // materialize the watchers based on the event
                watchers = watchManager.materialize(event.getState(), event.getType(), event.getPath());
            } else {
                watchers = new HashSet<>(materializedWatchers);
            }
            // 使用传入的ZK事件和ClientWatchManager生成事件和监听器的绑定对象
            WatcherSetEventPair pair = new WatcherSetEventPair(watchers, event);
            // queue the pair (watch set & event) for later processing
            // 将事件和监听器的绑定对象添加到waitingEvents集合中，这个集合类型只
            // 会是WatcherSetEventPair或者Packet
            waitingEvents.add(pair);
        }

        public void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            waitingEvents.add(new LocalCallback(cb, rc, path, ctx));
        }

        @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
        public void queuePacket(Packet packet) {
            if (wasKilled) {
                synchronized (waitingEvents) {
                    if (isRunning) {
                        waitingEvents.add(packet);
                    } else {
                        processEvent(packet);
                    }
                }
            } else {
                waitingEvents.add(packet);
            }
        }

        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        @Override
        @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
        public void run() {
            try {
                isRunning = true;
                while (true) {
                    // 轮询waitingEvents集合，取出其中的事件对象
                    Object event = waitingEvents.take();
                    // eventOfDeath为关闭事件
                    if (event == eventOfDeath) {
                        wasKilled = true;
                    } else {
                        // 不是关闭事件则开始处理事件
                        processEvent(event);
                    }
                    if (wasKilled) {
                        synchronized (waitingEvents) {
                            // 如果是关闭事件则会等waitingEvents全部处理之后再把
                            // EventThread设置为停止运行且退出循环
                            if (waitingEvents.isEmpty()) {
                                isRunning = false;
                                break;
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Event thread exiting due to interruption", e);
            }

            LOG.info("EventThread shut down for session: 0x{}", Long.toHexString(getSessionId()));
        }

        private void processEvent(Object event) {
            try {
                if (event instanceof WatcherSetEventPair) {
                    // each watcher will process the event
                    WatcherSetEventPair pair = (WatcherSetEventPair) event;
                    // 如果是正常的WatcherSetEventPair类型则直接取出里面所有的
                    // 监听器传入绑定的事件依次执行，这个步骤便是对应我们自己开发
                    // 的Watcher回调
                    for (Watcher watcher : pair.watchers) {
                        try {
                            watcher.process(pair.event);
                        } catch (Throwable t) {
                            LOG.error("Error while calling watcher.", t);
                        }
                    }
                } else if (event instanceof LocalCallback) {
                    LocalCallback lcb = (LocalCallback) event;
                    if (lcb.cb instanceof StatCallback) {
                        ((StatCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof DataCallback) {
                        ((DataCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ACLCallback) {
                        ((ACLCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ChildrenCallback) {
                        ((ChildrenCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof Children2Callback) {
                        ((Children2Callback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof StringCallback) {
                        ((StringCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof AsyncCallback.EphemeralsCallback) {
                        ((AsyncCallback.EphemeralsCallback) lcb.cb).processResult(lcb.rc, lcb.ctx, null);
                    } else if (lcb.cb instanceof AsyncCallback.AllChildrenNumberCallback) {
                        ((AsyncCallback.AllChildrenNumberCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, -1);
                    } else if (lcb.cb instanceof AsyncCallback.MultiCallback) {
                        ((AsyncCallback.MultiCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, Collections.emptyList());
                    } else {
                        ((VoidCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx);
                    }
                } else {
                    Packet p = (Packet) event;
                    int rc = 0;
                    String clientPath = p.clientPath;
                    if (p.replyHeader.getErr() != 0) {
                        rc = p.replyHeader.getErr();
                    }
                    if (p.cb == null) {
                        LOG.warn("Somehow a null cb got to EventThread!");
                    } else if (p.response instanceof ExistsResponse
                               || p.response instanceof SetDataResponse
                               || p.response instanceof SetACLResponse) {
                        StatCallback cb = (StatCallback) p.cb;
                        if (rc == Code.OK.intValue()) {
                            if (p.response instanceof ExistsResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((ExistsResponse) p.response).getStat());
                            } else if (p.response instanceof SetDataResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((SetDataResponse) p.response).getStat());
                            } else if (p.response instanceof SetACLResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((SetACLResponse) p.response).getStat());
                            }
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetDataResponse) {
                        DataCallback cb = (DataCallback) p.cb;
                        GetDataResponse rsp = (GetDataResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getData(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof GetACLResponse) {
                        ACLCallback cb = (ACLCallback) p.cb;
                        GetACLResponse rsp = (GetACLResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getAcl(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof GetChildrenResponse) {
                        ChildrenCallback cb = (ChildrenCallback) p.cb;
                        GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getChildren());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetAllChildrenNumberResponse) {
                        AllChildrenNumberCallback cb = (AllChildrenNumberCallback) p.cb;
                        GetAllChildrenNumberResponse rsp = (GetAllChildrenNumberResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getTotalNumber());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, -1);
                        }
                    } else if (p.response instanceof GetChildren2Response) {
                        Children2Callback cb = (Children2Callback) p.cb;
                        GetChildren2Response rsp = (GetChildren2Response) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getChildren(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof CreateResponse) {
                        StringCallback cb = (StringCallback) p.cb;
                        CreateResponse rsp = (CreateResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(
                                rc,
                                clientPath,
                                p.ctx,
                                (chrootPath == null
                                    ? rsp.getPath()
                                    : rsp.getPath().substring(chrootPath.length())));
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof Create2Response) {
                        Create2Callback cb = (Create2Callback) p.cb;
                        Create2Response rsp = (Create2Response) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(
                                    rc,
                                    clientPath,
                                    p.ctx,
                                    (chrootPath == null
                                            ? rsp.getPath()
                                            : rsp.getPath().substring(chrootPath.length())),
                                    rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof MultiResponse) {
                        MultiCallback cb = (MultiCallback) p.cb;
                        MultiResponse rsp = (MultiResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            List<OpResult> results = rsp.getResultList();
                            int newRc = rc;
                            for (OpResult result : results) {
                                if (result instanceof ErrorResult
                                    && KeeperException.Code.OK.intValue()
                                       != (newRc = ((ErrorResult) result).getErr())) {
                                    break;
                                }
                            }
                            cb.processResult(newRc, clientPath, p.ctx, results);
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetEphemeralsResponse) {
                        EphemeralsCallback cb = (EphemeralsCallback) p.cb;
                        GetEphemeralsResponse rsp = (GetEphemeralsResponse) p.response;
                        if (rc == Code.OK.intValue()) {
                            cb.processResult(rc, p.ctx, rsp.getEphemerals());
                        } else {
                            cb.processResult(rc, p.ctx, null);
                        }
                    } else if (p.cb instanceof VoidCallback) {
                        VoidCallback cb = (VoidCallback) p.cb;
                        cb.processResult(rc, clientPath, p.ctx);
                    }
                }
            } catch (Throwable t) {
                LOG.error("Unexpected throwable", t);
            }
        }

    }

    // @VisibleForTesting
    protected void finishPacket(Packet p) {
        int err = p.replyHeader.getErr();
        if (p.watchRegistration != null) {
            p.watchRegistration.register(err);
        }
        // Add all the removed watch events to the event queue, so that the
        // clients will be notified with 'Data/Child WatchRemoved' event type.
        if (p.watchDeregistration != null) {
            Map<EventType, Set<Watcher>> materializedWatchers = null;
            try {
                materializedWatchers = p.watchDeregistration.unregister(err);
                for (Entry<EventType, Set<Watcher>> entry : materializedWatchers.entrySet()) {
                    Set<Watcher> watchers = entry.getValue();
                    if (watchers.size() > 0) {
                        queueEvent(p.watchDeregistration.getClientPath(), err, watchers, entry.getKey());
                        // ignore connectionloss when removing from local
                        // session
                        p.replyHeader.setErr(Code.OK.intValue());
                    }
                }
            } catch (KeeperException.NoWatcherException nwe) {
                p.replyHeader.setErr(nwe.code().intValue());
            } catch (KeeperException ke) {
                p.replyHeader.setErr(ke.code().intValue());
            }
        }

        if (p.cb == null) {
            synchronized (p) {
                p.finished = true;
                p.notifyAll();
            }
        } else {
            p.finished = true;
            eventThread.queuePacket(p);
        }
    }

    void queueEvent(String clientPath, int err, Set<Watcher> materializedWatchers, EventType eventType) {
        KeeperState sessionState = KeeperState.SyncConnected;
        if (KeeperException.Code.SESSIONEXPIRED.intValue() == err
            || KeeperException.Code.CONNECTIONLOSS.intValue() == err) {
            sessionState = Event.KeeperState.Disconnected;
        }
        WatchedEvent event = new WatchedEvent(eventType, sessionState, clientPath);
        eventThread.queueEvent(event, materializedWatchers);
    }

    void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
        eventThread.queueCallback(cb, rc, path, ctx);
    }

    // for test only
    protected void onConnecting(InetSocketAddress addr) {

    }

    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }
        switch (state) {
        case AUTH_FAILED:
            p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
            break;
        case CLOSED:
            p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
            break;
        default:
            p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    private volatile long lastZxid;

    public long getLastZxid() {
        return lastZxid;
    }

    static class EndOfStreamException extends IOException {

        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }

    }

    private static class SessionTimeoutException extends IOException {

        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }

    }

    private static class SessionExpiredException extends IOException {

        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }

    }

    private static class RWServerFoundException extends IOException {

        private static final long serialVersionUID = 90431199887158758L;

        public RWServerFoundException(String msg) {
            super(msg);
        }

    }

    /**
     * This class services the outgoing request queue and generates the heart
     * beats. It also spawns the ReadThread.
     */
    class SendThread extends ZooKeeperThread {

        private long lastPingSentNs;
        // 客户端连接Server端的负责对象，默认采用的是NIO方式连接
        private final ClientCnxnSocket clientCnxnSocket;
        // 是否为第一次连接，默认是true
        private boolean isFirstConnect = true;
        private volatile ZooKeeperSaslClient zooKeeperSaslClient;


        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();

            // 将从Server端获取的ByteBuffer数据反序列化得到ReplyHeader
            replyHdr.deserialize(bbia, "header");
            switch (replyHdr.getXid()) {
            case PING_XID:
                // ping的xid为-2，因此会进入到这里面
                LOG.debug("Got ping response for session id: 0x{} after {}ms.",
                    Long.toHexString(sessionId),
                    ((System.nanoTime() - lastPingSentNs) / 1000000));
                // ping操作在这里面不会进行任何操作，而是直接退出，因此
                // readResponse()对ping没有任何作用
                return;
              case AUTHPACKET_XID:
                LOG.debug("Got auth session id: 0x{}", Long.toHexString(sessionId));
                if (replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    changeZkState(States.AUTH_FAILED);
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None,
                        Watcher.Event.KeeperState.AuthFailed, null));
                    eventThread.queueEventOfDeath();
                }
              return;
            case NOTIFICATION_XID:
                LOG.debug("Got notification session id: 0x{}",
                    Long.toHexString(sessionId));
                WatcherEvent event = new WatcherEvent();
                event.deserialize(bbia, "response");

                // convert from a server path to a client path
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if (serverPath.compareTo(chrootPath) == 0) {
                        event.setPath("/");
                    } else if (serverPath.length() > chrootPath.length()) {
                        event.setPath(serverPath.substring(chrootPath.length()));
                     } else {
                         LOG.warn("Got server path {} which is too short for chroot path {}.",
                             event.getPath(), chrootPath);
                     }
                }

                WatchedEvent we = new WatchedEvent(event);
                LOG.debug("Got {} for session id 0x{}", we, Long.toHexString(sessionId));
                eventThread.queueEvent(we);
                return;
            default:
                break;
            }

            // If SASL authentication is currently in progress, construct and
            // send a response packet immediately, rather than queuing a
            // response as with other packets.
            if (tunnelAuthInProgress()) {
                GetSASLRequest request = new GetSASLRequest();
                request.deserialize(bbia, "token");
                zooKeeperSaslClient.respondToServer(request.getToken(), ClientCnxn.this);
                return;
            }

            Packet packet;
            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got " + replyHdr.getXid());
                }
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             */
            try {
                if (packet.requestHeader.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got Xid " + replyHdr.getXid()
                                          + " with err " + replyHdr.getErr()
                                          + " expected Xid " + packet.requestHeader.getXid()
                                          + " for a packet with details: " + packet);
                }

                packet.replyHeader.setXid(replyHdr.getXid());
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());
                if (replyHdr.getZxid() > 0) {
                    lastZxid = replyHdr.getZxid();
                }
                if (packet.response != null && replyHdr.getErr() == 0) {
                    packet.response.deserialize(bbia, "response");
                }

                LOG.debug("Reading reply session id: 0x{}, packet:: {}", Long.toHexString(sessionId), packet);
            } finally {
                finishPacket(packet);
            }
        }

        SendThread(ClientCnxnSocket clientCnxnSocket) throws IOException {
            super(makeThreadName("-SendThread()"));
            changeZkState(States.CONNECTING);
            this.clientCnxnSocket = clientCnxnSocket;
            setDaemon(true);
        }

        // TODO: can not name this method getState since Thread.getState()
        // already exists
        // It would be cleaner to make class SendThread an implementation of
        // Runnable
        /**
         * Used by ClientCnxnSocket
         *
         * @return
         */
        synchronized ZooKeeper.States getZkState() {
            return state;
        }

        synchronized void changeZkState(ZooKeeper.States newState) throws IOException {
            if (!state.isAlive() && newState == States.CONNECTING) {
                throw new IOException(
                        "Connection has already been closed and reconnection is not allowed");
            }
            // It's safer to place state modification at the end.
            state = newState;
        }

        ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }

        /**
         * Setup session, previous watches, authentication.
         */
        void primeConnection() throws IOException {
            LOG.info(
                "Socket connection established, initiating session, client: {}, server: {}",
                clientCnxnSocket.getLocalSocketAddress(),
                clientCnxnSocket.getRemoteSocketAddress());
            // 调用了这个方法说明客户端和Server端的Socket长连接已经连接完毕了
            // 设置isFirstConnect为false
            isFirstConnect = false;
            long sessId = (seenRwServerBefore) ? sessionId : 0;
            // 创建连接的请求对象ConnectRequest
            ConnectRequest conReq = new ConnectRequest(0, lastZxid, sessionTimeout, sessId, sessionPasswd);
            // We add backwards since we are pushing into the front
            // Only send if there's a pending watch
            // disableAutoWatchReset对应着ZK的启动属性
            // zookeeper.disableAutoWatchReset，如果为false则为自动将ZK的
            // 监听器监听到相应的节点，为true则不会自动监听
            if (!clientConfig.getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET)) {
                // 接下来的流程大概就是从zooKeeper获取三种类型的监听器
                // 把三种类型的监听器依次封装成SetWatches包保存到
                // outgoingQueue包中以便后续发送包数据，具体的流程便忽略
                List<String> dataWatches = watchManager.getDataWatchList();
                List<String> existWatches = watchManager.getExistWatchList();
                List<String> childWatches = watchManager.getChildWatchList();
                List<String> persistentWatches = watchManager.getPersistentWatchList();
                List<String> persistentRecursiveWatches = watchManager.getPersistentRecursiveWatchList();
                if (!dataWatches.isEmpty() || !existWatches.isEmpty() || !childWatches.isEmpty()
                        || !persistentWatches.isEmpty() || !persistentRecursiveWatches.isEmpty()) {
                    Iterator<String> dataWatchesIter = prependChroot(dataWatches).iterator();
                    Iterator<String> existWatchesIter = prependChroot(existWatches).iterator();
                    Iterator<String> childWatchesIter = prependChroot(childWatches).iterator();
                    Iterator<String> persistentWatchesIter = prependChroot(persistentWatches).iterator();
                    Iterator<String> persistentRecursiveWatchesIter = prependChroot(persistentRecursiveWatches).iterator();
                    long setWatchesLastZxid = lastZxid;
                    // 轮询三种的迭代器获取迭代器具体数据
                    while (dataWatchesIter.hasNext() || existWatchesIter.hasNext() || childWatchesIter.hasNext()
                            || persistentWatchesIter.hasNext() || persistentRecursiveWatchesIter.hasNext()) {
                        List<String> dataWatchesBatch = new ArrayList<String>();
                        List<String> existWatchesBatch = new ArrayList<String>();
                        List<String> childWatchesBatch = new ArrayList<String>();
                        List<String> persistentWatchesBatch = new ArrayList<String>();
                        List<String> persistentRecursiveWatchesBatch = new ArrayList<String>();
                        int batchLength = 0;

                        // Note, we may exceed our max length by a bit when we add the last
                        // watch in the batch. This isn't ideal, but it makes the code simpler.
                        while (batchLength < SET_WATCHES_MAX_LENGTH) {
                            final String watch;
                            if (dataWatchesIter.hasNext()) {
                                watch = dataWatchesIter.next();
                                dataWatchesBatch.add(watch);
                            } else if (existWatchesIter.hasNext()) {
                                watch = existWatchesIter.next();
                                existWatchesBatch.add(watch);
                            } else if (childWatchesIter.hasNext()) {
                                watch = childWatchesIter.next();
                                childWatchesBatch.add(watch);
                            }  else if (persistentWatchesIter.hasNext()) {
                                watch = persistentWatchesIter.next();
                                persistentWatchesBatch.add(watch);
                            } else if (persistentRecursiveWatchesIter.hasNext()) {
                                watch = persistentRecursiveWatchesIter.next();
                                persistentRecursiveWatchesBatch.add(watch);
                            } else {
                                break;
                            }
                            batchLength += watch.length();
                        }

                        // 将获取到的监听器封装成SetWatches对象
                        Record record;
                        int opcode;
                        if (persistentWatchesBatch.isEmpty() && persistentRecursiveWatchesBatch.isEmpty()) {
                            // maintain compatibility with older servers - if no persistent/recursive watchers
                            // are used, use the old version of SetWatches
                            record = new SetWatches(setWatchesLastZxid, dataWatchesBatch, existWatchesBatch, childWatchesBatch);
                            opcode = OpCode.setWatches;
                        } else {
                            record = new SetWatches2(setWatchesLastZxid, dataWatchesBatch, existWatchesBatch,
                                    childWatchesBatch, persistentWatchesBatch, persistentRecursiveWatchesBatch);
                            opcode = OpCode.setWatches2;
                        }
                        RequestHeader header = new RequestHeader(ClientCnxn.SET_WATCHES_XID, opcode);
                        // 随后使用Packet封装Header和Recrod
                        Packet packet = new Packet(header, new ReplyHeader(), record, null, null);
                        // 添加到outgoingQueue数据中
                        outgoingQueue.addFirst(packet);
                    }
                }
            }

            for (AuthData id : authInfo) {
                outgoingQueue.addFirst(
                    new Packet(
                        new RequestHeader(ClientCnxn.AUTHPACKET_XID, OpCode.auth),
                        null,
                        new AuthPacket(0, id.scheme, id.data),
                        null,
                        null));
            }
            // 将ConnectRequest同样封装成Packet对象放到outgoingQueue中
            outgoingQueue.addFirst(new Packet(null, null, conReq, null, null, readOnly));
            // 开启OP_WRITE操作，开启后Selector.select()将可以收到读IO
            clientCnxnSocket.connectionPrimed();
            LOG.debug("Session establishment request sent on {}", clientCnxnSocket.getRemoteSocketAddress());
        }

        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        private void sendPing() {
            lastPingSentNs = System.nanoTime();
            // 创建xid为PING_XID的RequestHeader对象
            RequestHeader h = new RequestHeader(ClientCnxn.PING_XID, OpCode.ping);
            // ping请求只有RequestHeader有值，其它的都是null
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        private InetSocketAddress rwServerAddress = null;

        private static final int minPingRwTimeout = 100;

        private static final int maxPingRwTimeout = 60000;

        private int pingRwTimeout = minPingRwTimeout;

        // Set to true if and only if constructor of ZooKeeperSaslClient
        // throws a LoginException: see startConnect() below.
        private boolean saslLoginFailed = false;

        private void startConnect(InetSocketAddress addr) throws IOException {
            // initializing it for new connection
            saslLoginFailed = false;
            if (!isFirstConnect) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(1000));
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected exception", e);
                }
            }
            // 修改server状态
            changeZkState(States.CONNECTING);

            String hostPort = addr.getHostString() + ":" + addr.getPort();
            MDC.put("myid", hostPort);
            // 设置连接名称
            setName(getName().replaceAll("\\(.*\\)", "(" + hostPort + ")"));
            // 判断是否开启了SASL的客户端验证机制（C/S模式的验证机制）
            if (clientConfig.isSaslClientEnabled()) {
                try {
                    if (zooKeeperSaslClient != null) {
                        zooKeeperSaslClient.shutdown();
                    }
                    zooKeeperSaslClient = new ZooKeeperSaslClient(SaslServerPrincipal.getServerPrincipal(addr, clientConfig), clientConfig);
                } catch (LoginException e) {
                    // An authentication error occurred when the SASL client tried to initialize:
                    // for Kerberos this means that the client failed to authenticate with the KDC.
                    // This is different from an authentication error that occurs during communication
                    // with the Zookeeper server, which is handled below.
                    LOG.warn(
                        "SASL configuration failed. "
                            + "Will continue connection to Zookeeper server without "
                            + "SASL authentication, if Zookeeper server allows it.", e);
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.AuthFailed, null));
                    saslLoginFailed = true;
                }
            }
            // 进行连接的日志打印
            logStartConnect(addr);
            // 调用clientCnxnSocket的连接方法
            clientCnxnSocket.connect(addr);
        }

        private void logStartConnect(InetSocketAddress addr) {
            LOG.info("Opening socket connection to server {}.", addr);
            if (zooKeeperSaslClient != null) {
                LOG.info("SASL config status: {}", zooKeeperSaslClient.getConfigStatus());
            }
        }

        @Override
        @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
        public void run() {
            // 更新clientCnxnSocket的发送事件以及关联SendTreahd，这里sessionId
            // 没有值，就是0
            clientCnxnSocket.introduce(this, sessionId, outgoingQueue);
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
            // 上次ping和现在的时间差
            int to;
            long lastPingRwServer = Time.currentElapsedTime();

            final int MAX_SEND_PING_INTERVAL = 10000; //10 seconds
            InetSocketAddress serverAddress = null;
            // 只要连接没有关闭，也没有验证失败，就一直循环
            while (state.isAlive()) {
                try {
                    // 刚开始运行时这里肯定是未连接的状态，因此会进去
                    if (!clientCnxnSocket.isConnected()) {
                        // don't re-establish connection if we are closing
                        // 如果ZK已经关闭了则直接会出循环
                        if (closing) {
                            break;
                        }
                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        } else {
                            // 获取要连接的server的址
                            serverAddress = hostProvider.next(1000);
                        }
                        onConnecting(serverAddress);
                        // 开始连接
                        startConnect(serverAddress);
                        // Update now to start the connection timer right after we make a connection attempt
                        clientCnxnSocket.updateNow();
                        // 更新交互（连接请求/读写请求）时间戳
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    if (state.isConnected()) {
                        // determine whether we need to send an AuthFailed event.
                        if (zooKeeperSaslClient != null) {
                            boolean sendAuthEvent = false;
                            if (zooKeeperSaslClient.getSaslState() == ZooKeeperSaslClient.SaslState.INITIAL) {
                                try {
                                    zooKeeperSaslClient.initialize(ClientCnxn.this);
                                } catch (SaslException e) {
                                    LOG.error("SASL authentication with Zookeeper Quorum member failed.", e);
                                    changeZkState(States.AUTH_FAILED);
                                    sendAuthEvent = true;
                                }
                            }
                            KeeperState authState = zooKeeperSaslClient.getKeeperState();
                            if (authState != null) {
                                if (authState == KeeperState.AuthFailed) {
                                    // An authentication error occurred during authentication with the Zookeeper Server.
                                    changeZkState(States.AUTH_FAILED);
                                    sendAuthEvent = true;
                                } else {
                                    if (authState == KeeperState.SaslAuthenticated) {
                                        sendAuthEvent = true;
                                    }
                                }
                            }

                            if (sendAuthEvent) {
                                eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, authState, null));
                                if (state == States.AUTH_FAILED) {
                                    eventThread.queueEventOfDeath();
                                }
                            }
                        }
                        // 连接上之后使用的属性变成了readTimeout，getIdleRecv()
                        // 方法使用的属性为lastHeard，即最后一次监听到服务端响应
                        // 的时间戳
                        // 获取已经有多久没有收到交互响应了
                        to = readTimeout - clientCnxnSocket.getIdleRecv();
                    } else {
                        // 未连接时会进入，因此ping流程这里不会使用，可以得出结论
                        // connectTime属性只会在新建连接时被使用
                        // 连接上之后失去作用
                        // 获取已经有多久没有收到连接请求的响应了
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }

                    // 处理会话超时的情况
                    if (to <= 0) {
                        String warnInfo = String.format(
                            "Client session timed out, have not heard from server in %dms for session id 0x%s",
                            clientCnxnSocket.getIdleRecv(),
                            Long.toHexString(sessionId));
                        LOG.warn(warnInfo);
                        // 如果进入到这里面，说明readTimeout或者connectTimeout
                        // 要小于上次监听到Server端的时间间隔，意味着时间过期
                        // 抛出会话超时异常
                        throw new SessionTimeoutException(warnInfo);
                    }
                    if (state.isConnected()) {
                        //1000(1 second) is to prevent race condition missing to send the second ping
                        //also make sure not to send too many pings when readTimeout is small
                        // 获取下次ping的时间，也可以说获取select()最大阻塞时间
                        // 这个公式分两个情况：
                        // 1、lastSend距今超过1000ms(1s)，则固定减去1000ms
                        // 具体公式表现为：(readTimeout / 2) - idleSend - 1000
                        // 2、lastSend距今小于等于1000ms，则不做任何操作
                        // 具体公式表现为：(readTimeout / 2) - idleSend
                        int timeToNextPing = readTimeout / 2
                                             - clientCnxnSocket.getIdleSend()
                                             - ((clientCnxnSocket.getIdleSend() > 1000) ? 1000 : 0);
                        //send a ping request either time is due or no packet sent out within MAX_SEND_PING_INTERVAL
                        // 如果timeToNextPing小于等于0或者idleSend间隔超过10s
                        // 说明是时候该发送ping请求确认连接了
                        if (timeToNextPing <= 0 || clientCnxnSocket.getIdleSend() > MAX_SEND_PING_INTERVAL) {
                            // 发送ping请求包
                            sendPing();
                            // 更新lastSend属性
                            clientCnxnSocket.updateLastSend();
                        } else {
                            // to在前面设的值
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }

                    // If we are in read-only mode, seek for read/write server
                    if (state == States.CONNECTEDREADONLY) {
                        long now = Time.currentElapsedTime();
                        int idlePingRwServer = (int) (now - lastPingRwServer);
                        if (idlePingRwServer >= pingRwTimeout) {
                            lastPingRwServer = now;
                            idlePingRwServer = 0;
                            pingRwTimeout = Math.min(2 * pingRwTimeout, maxPingRwTimeout);
                            pingRwServer();
                        }
                        to = Math.min(to, pingRwTimeout - idlePingRwServer);
                    }
                    // 这个方法十分重要，因为不管是连接还是其它任何操作都会进入
                    // 该方法进行操作类型判断已经发送接收数据包，具体流程留到
                    // 后续分析clientCnxnSocket对象时再看

                    // 要发送ping请求这个方法可能将会被调用两次，第一次是在
                    // sendPing()之后调用，如果是OP_WRITE操作则可以立马进行写操作
                    // 如果不是则会在第一次调用时开启OP_WRITE操作，轮询第二次的时候
                    // 再调用一次用来发送ping数据包
                    clientCnxnSocket.doTransport(to, pendingQueue, ClientCnxn.this);
                } catch (Throwable e) {
                    if (closing) {
                        // closing so this is expected
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                "An exception was thrown while closing send thread for session 0x{}.",
                                Long.toHexString(getSessionId()), e);
                        }
                        break;
                    } else {
                        LOG.warn(
                            "Session 0x{} for server {}, Closing socket connection. "
                                + "Attempting reconnect except it is a SessionExpiredException.",
                            Long.toHexString(getSessionId()),
                            serverAddress,
                            e);

                        // At this point, there might still be new packets appended to outgoingQueue.
                        // they will be handled in next connection or cleared up if closed.
                        cleanAndNotifyState();
                    }
                }
            }
            // 跑到这里说明ZK已经关闭了，后面会做一些善后的工作，如发送关闭事件
            // 清除连接的缓存数据等
            synchronized (outgoingQueue) {
                // When it comes to this point, it guarantees that later queued
                // packet to outgoingQueue will be notified of death.
                cleanup();
            }
            clientCnxnSocket.close();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Disconnected, null));
            }
            eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Closed, null));

            if (zooKeeperSaslClient != null) {
                zooKeeperSaslClient.shutdown();
            }
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.getTextTraceLevel(),
                "SendThread exited loop for session: 0x" + Long.toHexString(getSessionId()));
        }

        private void cleanAndNotifyState() {
            cleanup();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Disconnected, null));
            }
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
        }

        private void pingRwServer() throws RWServerFoundException {
            String result = null;
            InetSocketAddress addr = hostProvider.next(0);

            LOG.info("Checking server {} for being r/w. Timeout {}", addr, pingRwTimeout);

            Socket sock = null;
            BufferedReader br = null;
            try {
                sock = new Socket(addr.getHostString(), addr.getPort());
                sock.setSoLinger(false, -1);
                sock.setSoTimeout(1000);
                sock.setTcpNoDelay(true);
                sock.getOutputStream().write("isro".getBytes());
                sock.getOutputStream().flush();
                sock.shutdownOutput();
                br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                result = br.readLine();
            } catch (ConnectException e) {
                // ignore, this just means server is not up
            } catch (IOException e) {
                // some unexpected error, warn about it
                LOG.warn("Exception while seeking for r/w server.", e);
            } finally {
                if (sock != null) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }

            if ("rw".equals(result)) {
                pingRwTimeout = minPingRwTimeout;
                // save the found address so that it's used during the next
                // connection attempt
                rwServerAddress = addr;
                throw new RWServerFoundException("Majority server found at "
                                                 + addr.getHostString() + ":" + addr.getPort());
            }
        }

        private void cleanup() {
            clientCnxnSocket.cleanup();
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }
            // We can't call outgoingQueue.clear() here because
            // between iterating and clear up there might be new
            // packets added in queuePacket().
            Iterator<Packet> iter = outgoingQueue.iterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                conLossPacket(p);
                iter.remove();
            }
        }

        /**
         * Callback invoked by the ClientCnxnSocket once a connection has been
         * established.
         *
         * @param _negotiatedSessionTimeout
         * @param _sessionId
         * @param _sessionPasswd
         * @param isRO
         * @throws IOException
         */
        void onConnected(
            int _negotiatedSessionTimeout,
            long _sessionId,
            byte[] _sessionPasswd,
            boolean isRO) throws IOException {
            // _negotiatedSessionTimeout便是Client端和Server端互相协商获得的
            // sessionTimeout过期时间
            negotiatedSessionTimeout = _negotiatedSessionTimeout;
            // 时间小于等于0说明连接失败了
            if (negotiatedSessionTimeout <= 0) {
                changeZkState(States.CLOSED);
                // 发送ZK过期事件
                eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
                // 并且发送停止服务事件
                eventThread.queueEventOfDeath();

                String warnInfo = String.format(
                    "Unable to reconnect to ZooKeeper service, session 0x%s has expired",
                    Long.toHexString(sessionId));
                LOG.warn(warnInfo);
                throw new SessionExpiredException(warnInfo);
            }

            if (!readOnly && isRO) {
                LOG.error("Read/write client got connected to read-only server");
            }

            // 接下来便是设值了，具体的值在这里都可以看到
            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / hostProvider.size();
            hostProvider.onConnected();
            sessionId = _sessionId;
            sessionPasswd = _sessionPasswd;
            // 根据Server端传来的属性设值状态
            changeZkState((isRO) ? States.CONNECTEDREADONLY : States.CONNECTED);

            seenRwServerBefore |= !isRO;
            LOG.info(
                "Session establishment complete on server {}, session id = 0x{}, negotiated timeout = {}{}",
                clientCnxnSocket.getRemoteSocketAddress(),
                Long.toHexString(sessionId),
                negotiatedSessionTimeout,
                (isRO ? " (READ-ONLY mode)" : ""));
            // 确定等下要发送的事件类型
            KeeperState eventState = (isRO) ? KeeperState.ConnectedReadOnly : KeeperState.SyncConnected;
            // 使用EventThread线程对象发布监听事件
            eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, eventState, null));
        }

        void close() {
            try {
                changeZkState(States.CLOSED);
            } catch (IOException e) {
                LOG.warn("Connection close fails when migrates state from {} to CLOSED",
                        getZkState());
            }
            clientCnxnSocket.onClosing();
        }

        void testableCloseSocket() throws IOException {
            clientCnxnSocket.testableCloseSocket();
        }

        public boolean tunnelAuthInProgress() {
            // 1. SASL client is disabled.
            if (!clientConfig.isSaslClientEnabled()) {
                return false;
            }

            // 2. SASL login failed.
            if (saslLoginFailed) {
                return false;
            }

            // 3. SendThread has not created the authenticating object yet,
            // therefore authentication is (at the earliest stage of being) in progress.
            if (zooKeeperSaslClient == null) {
                return true;
            }

            // 4. authenticating object exists, so ask it for its progress.
            return zooKeeperSaslClient.clientTunneledAuthenticationInProgress();
        }

        public void sendPacket(Packet p) throws IOException {
            clientCnxnSocket.sendPacket(p);
        }

        public ZooKeeperSaslClient getZooKeeperSaslClient() {
            return zooKeeperSaslClient;
        }
    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     */
    public void disconnect() {
        LOG.debug("Disconnecting client for session: 0x{}", Long.toHexString(getSessionId()));

        sendThread.close();
        try {
            sendThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted while waiting for the sender thread to close", ex);
        }
        eventThread.queueEventOfDeath();
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        LOG.debug("Closing client for session: 0x{}", Long.toHexString(getSessionId()));

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    // @VisibleForTesting
    protected int xid = 1;

    // @VisibleForTesting
    volatile States state = States.NOT_CONNECTED;

    /*
     * getXid() is called externally by ClientCnxnNIO::doIO() when packets are sent from the outgoingQueue to
     * the server. Thus, getXid() must be public.
     */
    public synchronized int getXid() {
        // Avoid negative cxid values.  In particular, cxid values of -4, -2, and -1 are special and
        // must not be used for requests -- see SendThread.readResponse.
        // Skip from MAX to 1.
        if (xid == Integer.MAX_VALUE) {
            xid = 1;
        }
        return xid++;
    }

    public ReplyHeader submitRequest(
        RequestHeader h,
        Record request,
        Record response,
        WatchRegistration watchRegistration) throws InterruptedException {
        return submitRequest(h, request, response, watchRegistration, null);
    }

    public ReplyHeader submitRequest(
        RequestHeader h,
        Record request,
        Record response,
        WatchRegistration watchRegistration,
        WatchDeregistration watchDeregistration) throws InterruptedException {
        ReplyHeader r = new ReplyHeader();
        Packet packet = queuePacket(
            h,
            r,
            request,
            response,
            null,
            null,
            null,
            null,
            watchRegistration,
            watchDeregistration);
        synchronized (packet) {
            if (requestTimeout > 0) {
                // Wait for request completion with timeout
                waitForPacketFinish(r, packet);
            } else {
                // Wait for request completion infinitely
                while (!packet.finished) {
                    packet.wait();
                }
            }
        }
        if (r.getErr() == Code.REQUESTTIMEOUT.intValue()) {
            sendThread.cleanAndNotifyState();
        }
        return r;
    }

    /**
     * Wait for request completion with timeout.
     */
    private void waitForPacketFinish(ReplyHeader r, Packet packet) throws InterruptedException {
        long waitStartTime = Time.currentElapsedTime();
        while (!packet.finished) {
            packet.wait(requestTimeout);
            if (!packet.finished && ((Time.currentElapsedTime() - waitStartTime) >= requestTimeout)) {
                LOG.error("Timeout error occurred for the packet '{}'.", packet);
                r.setErr(Code.REQUESTTIMEOUT.intValue());
                break;
            }
        }
    }

    public void saslCompleted() {
        sendThread.getClientCnxnSocket().saslCompleted();
    }

    public void sendPacket(Record request, Record response, AsyncCallback cb, int opCode) throws IOException {
        // Generate Xid now because it will be sent immediately,
        // by call to sendThread.sendPacket() below.
        int xid = getXid();
        RequestHeader h = new RequestHeader();
        h.setXid(xid);
        h.setType(opCode);

        ReplyHeader r = new ReplyHeader();
        r.setXid(xid);

        Packet p = new Packet(h, r, request, response, null, false);
        p.cb = cb;
        sendThread.sendPacket(p);
    }

    public Packet queuePacket(
        RequestHeader h,
        ReplyHeader r,
        Record request,
        Record response,
        AsyncCallback cb,
        String clientPath,
        String serverPath,
        Object ctx,
        WatchRegistration watchRegistration) {
        return queuePacket(h, r, request, response, cb, clientPath, serverPath, ctx, watchRegistration, null);
    }

    @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    public Packet queuePacket(
        RequestHeader h,
        ReplyHeader r,
        Record request,
        Record response,
        AsyncCallback cb,
        String clientPath,
        String serverPath,
        Object ctx,
        WatchRegistration watchRegistration,
        WatchDeregistration watchDeregistration) {
        Packet packet = null;

        // Note that we do not generate the Xid for the packet yet. It is
        // generated later at send-time, by an implementation of ClientCnxnSocket::doIO(),
        // where the packet is actually sent.
        packet = new Packet(h, r, request, response, watchRegistration);
        packet.cb = cb;
        packet.ctx = ctx;
        packet.clientPath = clientPath;
        packet.serverPath = serverPath;
        packet.watchDeregistration = watchDeregistration;
        // The synchronized block here is for two purpose:
        // 1. synchronize with the final cleanup() in SendThread.run() to avoid race
        // 2. synchronized against each packet. So if a closeSession packet is added,
        // later packet will be notified.
        // 方法的大致作用便是将前面传进来的RequestHeader对象封装成Packet对象
        // 并最终放入outgoingQueue数组等待下次发送数据包时发送
        synchronized (outgoingQueue) {
            if (!state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // If the client is asking to close the session then
                // mark as closing
                if (h.getType() == OpCode.closeSession) {
                    closing = true;
                }
                // 加到outgoingQueue
                outgoingQueue.add(packet);
            }
        }
        // 调用selector.wakeup()方法来唤醒select()方法，调用这个方法的作用
        // 便是防止将ping数据包放到outgoingQueue后再次被select()方法阻塞从而
        // 直接调用阻塞方法的后面逻辑
        sendThread.getClientCnxnSocket().packetAdded();
        return packet;
    }

    public void addAuthInfo(String scheme, byte[] auth) {
        if (!state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(
            new RequestHeader(ClientCnxn.AUTHPACKET_XID, OpCode.auth),
            null,
            new AuthPacket(0, scheme, auth),
            null,
            null,
            null,
            null,
            null,
            null);
    }

    States getState() {
        return state;
    }

    private static class LocalCallback {

        private final AsyncCallback cb;
        private final int rc;
        private final String path;
        private final Object ctx;

        public LocalCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            this.cb = cb;
            this.rc = rc;
            this.path = path;
            this.ctx = ctx;
        }

    }

    private void initRequestTimeout() {
        try {
            requestTimeout = clientConfig.getLong(
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT,
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT_DEFAULT);
            LOG.info(
                "{} value is {}. feature enabled={}",
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT,
                requestTimeout,
                requestTimeout > 0);
        } catch (NumberFormatException e) {
            LOG.error(
                "Configured value {} for property {} can not be parsed to long.",
                clientConfig.getProperty(ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT),
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT);
            throw e;
        }
    }

    public ZooKeeperSaslClient getZooKeeperSaslClient() {
        return sendThread.getZooKeeperSaslClient();
    }
}
