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

package org.apache.zookeeper.server.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.zookeeper.common.NetUtils.formatInetAddr;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLSocket;
import org.apache.zookeeper.common.NetUtils;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.util.CircularBlockingQueue;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 *
 * 翻译：这个类为使用TCP的领袖选举实现了一个连接管理器。
 *  * 它为每对服务器维护一个连接。棘手的部分是确保每一对服务器都有一个连接，
 *  * 这些服务器都在正确地运行，并且可以通过网络进行通信。
 *  *
 *  * 解释：每队服务器维护一个连接的意思就是A连接服务器
 *
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties.
 *
 * 翻译：如果两个服务器试图同时启动一个连接，则连接管理器使用非常简单的中断连接
 *  * 机制来决定哪个中断，基于双方的IP地址。
 *
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 * 翻译：对于每个对等体，管理器维护着一个消息发送队列。如果连接到任何
 *  * 特定的Server中断，那么发送者线程将消息放回到这个队列中。
 *  * 作为这个实现，当前使用一个队列来实现维护发送给另一方的消息，因此我们将消息
 *  * 添加到队列的尾部，从而更改了消息的顺序。虽然对于Leader选举来说这不是一个问题，
 *  * 但对于加强对等通信可能就是个问题。不过，这一点有待验证。
 *  *
 *  * 解释：比如发送消息1给某一个Server，如果和该Server连接断开后，想再给该Server发送消息2时
 *  * 此时消息就会存到该Server在本地对应维护的一个消息发送队列中，等连接恢复后会重新尝试发送。
 */

// 每个server都拥有一个QuorumCnxManager，而QuorumCnxManager对象拥有一个消息发送map，
// 该map的key为其它server的id，value为一个队列。这个队列中存放的是当前server向这个server
// 发送失败的消息。这样的话，对于这个map会有这样的三种情况：
// 1)所有队列均为空：说明当前server发送的消息全部成功。
// 2)所有队列均不空：说明当前server发送给所有其它server的消息全部失败，即当前server与集合失联。
// 3)若有一个队列为空：说明当前server给这个server发送的消息全部成功，即当前server与集群没有失联。
public class QuorumCnxManager {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * Maximum capacity of thread queues
     */
    // 接收集合recvQueue的容量
    static final int RECV_CAPACITY = 100;
    // Initialized to 1 to prevent sending
    // stale notifications to peers
    // 每次发送消息的数量，固定是一，确保消息可以有序安全的发送出去
    static final int SEND_CAPACITY = 1;

    // 接收目标机器发送过来的数据最大长度，最大长度为500K
    static final int PACKETMAXSIZE = 1024 * 512;

    /*
     * Negative counter for observer server ids.
     */

    private AtomicLong observerCounter = new AtomicLong(-1);

    /*
     * Protocol identifier used among peers (must be a negative number for backward compatibility reasons)
     */
    // the following protocol version was sent in every connection initiation message since ZOOKEEPER-107 released in 3.5.0
    public static final long PROTOCOL_VERSION_V1 = -65536L;
    // ZOOKEEPER-3188 introduced multiple addresses in the connection initiation message, released in 3.6.0
    public static final long PROTOCOL_VERSION_V2 = -65535L;

    /*
     * Max buffer size to be read from the network.
     */
    public static final int maxBuffer = 2048;

    /*
     * Connection time out value in milliseconds
     */

    private int cnxTO = 5000;

    final QuorumPeer self;

    /*
     * Local IP address
     */
    final long mySid;
    final int socketTimeout;
    final Map<Long, QuorumPeer.QuorumServer> view;
    final boolean listenOnAllIPs;
    private ThreadPoolExecutor connectionExecutor;
    private final Set<Long> inprogressConnections = Collections.synchronizedSet(new HashSet<>());
    private QuorumAuthServer authServer;
    private QuorumAuthLearner authLearner;
    private boolean quorumSaslAuthEnabled;
    /*
     * Counter to count connection processing threads.
     */
    private AtomicInteger connectionThreadCnt = new AtomicInteger(0);

    /*
     * Mapping from Peer to Thread number
     */
    //每一个QuorumPeer都有一个QuorumCnxManager对象负责选举期间QuorumPeer之间连接的
    //建立和发送、接收消息队列的维护，而这些消息是通过以下4个集合被处理的：

    // 保存和集群内另一台机器通信对的集合，key为另一台机器的myid，value则是
    // 本机器与其通信的通信对
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    // 将要发送给某个机器的ByteBuffer集合，key为发送机器的sid，value为单个消息
    // 元素的阻塞队列，确保每次只发送一条消息（ArrayBlockingQueue长度固定）
    final ConcurrentHashMap<Long, BlockingQueue<ByteBuffer>> queueSendMap;
    // 保存给sid机器的最后发送消息，key为目标机器的sid，value则是具体的发送消息
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    /*
     * Reception queue
     */
    // QuorumCnxManager对象和外界对象进行交互消息交互的集合中介，往这个集合中
    // 放入数据说明一个问题：RecvWorker已经收到了其它机器的消息并处理转换完成
    public final BlockingQueue<Message> recvQueue;

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    // 记录当前的连接管理对象中有多少个线程正在运行，即选择通信对的
    // SendWorker和RecvWorker
    private AtomicInteger threadCnt = new AtomicInteger(0);

    /*
     * Socket options for TCP keepalive
     */
    private final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");


    /*
     * Socket factory, allowing the injection of custom socket implementations for testing
     */
    static final Supplier<Socket> DEFAULT_SOCKET_FACTORY = () -> new Socket();
    private static Supplier<Socket> SOCKET_FACTORY = DEFAULT_SOCKET_FACTORY;
    static void setSocketFactory(Supplier<Socket> factory) {
        SOCKET_FACTORY = factory;
    }


    public static class Message {

        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;

    }

    /*
     * This class parses the initial identification sent out by peers with their
     * sid & hostname.
     */
    public static class InitialMessage {

        public Long sid;
        public List<InetSocketAddress> electionAddr;

        InitialMessage(Long sid, List<InetSocketAddress> addresses) {
            this.sid = sid;
            this.electionAddr = addresses;
        }

        @SuppressWarnings("serial")
        public static class InitialMessageException extends Exception {

            InitialMessageException(String message, Object... args) {
                super(String.format(message, args));
            }

        }

        public static InitialMessage parse(Long protocolVersion, DataInputStream din) throws InitialMessageException, IOException {
            Long sid;

            if (protocolVersion != PROTOCOL_VERSION_V1 && protocolVersion != PROTOCOL_VERSION_V2) {
                throw new InitialMessageException("Got unrecognized protocol version %s", protocolVersion);
            }
            // 如果是版本号则再次读取sid
            sid = din.readLong();
            // 判断是否有剩余的数组需要读取
            int remaining = din.readInt();
            // 如果接下来的数据长度为负数或者大于了最大缓存值2048字节
            // 则说明有问题，需要关闭连接（值如果是0也是OK的）
            if (remaining <= 0 || remaining > maxBuffer) {
                throw new InitialMessageException("Unreasonable buffer length: %s", remaining);
            }
            // 将读取到的长度实例化一个数组并将剩余的读取完
            byte[] b = new byte[remaining];
            int num_read = din.read(b);

            if (num_read != remaining) {
                throw new InitialMessageException("Read only %s bytes out of %s sent by server %s", num_read, remaining, sid);
            }

            // in PROTOCOL_VERSION_V1 we expect to get a single address here represented as a 'host:port' string
            // in PROTOCOL_VERSION_V2 we expect to get multiple addresses like: 'host1:port1|host2:port2|...'
            String[] addressStrings = new String(b, UTF_8).split("\\|");
            List<InetSocketAddress> addresses = new ArrayList<>(addressStrings.length);
            for (String addr : addressStrings) {

                String[] host_port;
                try {
                    host_port = ConfigUtils.getHostAndPort(addr);
                } catch (ConfigException e) {
                    throw new InitialMessageException("Badly formed address: %s", addr);
                }

                if (host_port.length != 2) {
                    throw new InitialMessageException("Badly formed address: %s", addr);
                }

                int port;
                try {
                    port = Integer.parseInt(host_port[1]);
                } catch (NumberFormatException e) {
                    throw new InitialMessageException("Bad port number: %s", host_port[1]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new InitialMessageException("No port number in: %s", addr);
                }
                if (!isWildcardAddress(host_port[0])) {
                    addresses.add(new InetSocketAddress(host_port[0], port));
                }
            }

            return new InitialMessage(sid, addresses);
        }

        /**
         * Returns true if the specified hostname is a wildcard address,
         * like 0.0.0.0 for IPv4 or :: for IPv6
         *
         * (the function is package-private to be visible for testing)
         */
        static boolean isWildcardAddress(final String hostname) {
            try {
                return InetAddress.getByName(hostname).isAnyLocalAddress();
            } catch (UnknownHostException e) {
                // if we can not resolve, it can not be a wildcard address
                return false;
            }
        }

        @Override
        public String toString() {
            return "InitialMessage{sid=" + sid + ", electionAddr=" + electionAddr + '}';
        }
    }

    public QuorumCnxManager(QuorumPeer self, final long mySid, Map<Long, QuorumPeer.QuorumServer> view,
        QuorumAuthServer authServer, QuorumAuthLearner authLearner, int socketTimeout, boolean listenOnAllIPs,
        int quorumCnxnThreadsSize, boolean quorumSaslAuthEnabled) {

        // 初始化recvQueue、queueSendMap、和senderWorkerMap集合对象，用来
        // 接收和集群间的机器消息信息
        this.recvQueue = new CircularBlockingQueue<>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<>();
        this.senderWorkerMap = new ConcurrentHashMap<>();
        this.lastMessageSent = new ConcurrentHashMap<>();

        // 读取选举时调用方法connect()连接其它机器的选举通信Socket时的参数
        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if (cnxToValue != null) {
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.self = self;

        this.mySid = mySid;
        this.socketTimeout = socketTimeout;
        this.view = view;
        this.listenOnAllIPs = listenOnAllIPs;
        this.authServer = authServer;
        this.authLearner = authLearner;
        this.quorumSaslAuthEnabled = quorumSaslAuthEnabled;

        initializeConnectionExecutor(mySid, quorumCnxnThreadsSize);

        // Starts listener thread that waits for connection requests
        // 创建监听其它机器连接本机器的Socket监听器
        listener = new Listener();
        listener.setName("QuorumPeerListener");
    }

    // we always use the Connection Executor during connection initiation (to handle connection
    // timeouts), and optionally use it during receiving connections (as the Quorum SASL authentication
    // can take extra time)
    private void initializeConnectionExecutor(final long mySid, final int quorumCnxnThreadsSize) {
        final AtomicInteger threadIndex = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

        final ThreadFactory daemonThFactory = runnable -> new Thread(group, runnable,
            String.format("QuorumConnectionThread-[myid=%d]-%d", mySid, threadIndex.getAndIncrement()));

        this.connectionExecutor = new ThreadPoolExecutor(3, quorumCnxnThreadsSize, 60, TimeUnit.SECONDS,
                                                         new SynchronousQueue<>(), daemonThFactory);
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }

    /**
     * Invokes initiateConnection for testing purposes
     *
     * @param sid
     */
    public void testInitiateConnection(long sid) {
        LOG.debug("Opening channel to server {}", sid);
        initiateConnection(self.getVotingView().get(sid).electionAddr, sid);
    }

    /**
     * First we create the socket, perform SSL handshake and authentication if needed.
     * Then we perform the initiation protocol.
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    public void initiateConnection(final MultipleAddresses electionAddr, final Long sid) {
        // 创建Socket对象
        Socket sock = null;
        try {
            LOG.debug("Opening channel to server {}", sid);
            if (self.isSslQuorum()) {
                sock = self.getX509Util().createSSLSocket();
            } else {
                sock = SOCKET_FACTORY.get();
            }
            // 在这个方法中设置timeout
            setSockOpts(sock);
            // 连接另外一台参与选举的机器，并且设置连接时间为5s
            sock.connect(electionAddr.getReachableOrOne(), cnxTO);
            if (sock instanceof SSLSocket) {
                SSLSocket sslSock = (SSLSocket) sock;
                sslSock.startHandshake();
                LOG.info("SSL handshake complete with {} - {} - {}",
                         sslSock.getRemoteSocketAddress(),
                         sslSock.getSession().getProtocol(),
                         sslSock.getSession().getCipherSuite());
            }

            LOG.debug("Connected to server {} using election address: {}:{}",
                      sid, sock.getInetAddress(), sock.getPort());
        } catch (X509Exception e) {
            LOG.warn("Cannot open secure channel to {} at election address {}", sid, electionAddr, e);
            closeSocket(sock);
            return;
        } catch (UnresolvedAddressException | IOException e) {
            LOG.warn("Cannot open channel to {} at election address {}", sid, electionAddr, e);
            closeSocket(sock);
            return;
        }

        try {
            // todo
            startConnection(sock, sid);
        } catch (IOException e) {
            LOG.error(
              "Exception while connecting, id: {}, addr: {}, closing learner connection",
              sid,
              sock.getRemoteSocketAddress(),
              e);
            closeSocket(sock);
        }
    }

    /**
     * Server will initiate the connection request to its peer server
     * asynchronously via separate connection thread.
     */
    public boolean initiateConnectionAsync(final MultipleAddresses electionAddr, final Long sid) {
        if (!inprogressConnections.add(sid)) {
            // simply return as there is a connection request to
            // server 'sid' already in progress.
            LOG.debug("Connection request to server id: {} is already in progress, so skipping this request", sid);
            return true;
        }
        try {
            connectionExecutor.execute(new QuorumConnectionReqThread(electionAddr, sid));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            // Imp: Safer side catching all type of exceptions and remove 'sid'
            // from inprogress connections. This is to avoid blocking further
            // connection requests from this 'sid' in case of errors.
            inprogressConnections.remove(sid);
            LOG.error("Exception while submitting quorum connection request", e);
            return false;
        }
        return true;
    }

    /**
     * Thread to send connection request to peer server.
     */
    private class QuorumConnectionReqThread extends ZooKeeperThread {
        final MultipleAddresses electionAddr;
        final Long sid;
        QuorumConnectionReqThread(final MultipleAddresses electionAddr, final Long sid) {
            super("QuorumConnectionReqThread-" + sid);
            this.electionAddr = electionAddr;
            this.sid = sid;
        }

        @Override
        public void run() {
            try {
                initiateConnection(electionAddr, sid);
            } finally {
                inprogressConnections.remove(sid);
            }
        }

    }

    private boolean startConnection(Socket sock, Long sid) throws IOException {
        DataOutputStream dout = null;
        DataInputStream din = null;
        LOG.debug("startConnection (myId:{} --> sid:{})", self.getId(), sid);
        try {
            // Use BufferedOutputStream to reduce the number of IP packets. This is
            // important for x-DC scenarios.
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            // 连接上另一台myid为sid的机器后立马向其发送本机器的myid
            dout = new DataOutputStream(buf);

            // Sending id and challenge

            // First sending the protocol version (in other words - message type).
            // For backward compatibility reasons we stick to the old protocol version, unless the MultiAddress
            // feature is enabled. During rolling upgrade, we must make sure that all the servers can
            // understand the protocol version we use to avoid multiple partitions. see ZOOKEEPER-3720
            long protocolVersion = self.isMultiAddressEnabled() ? PROTOCOL_VERSION_V2 : PROTOCOL_VERSION_V1;
            dout.writeLong(protocolVersion);
            dout.writeLong(self.getId());

            // now we send our election address. For the new protocol version, we can send multiple addresses.
            Collection<InetSocketAddress> addressesToSend = protocolVersion == PROTOCOL_VERSION_V2
                    ? self.getElectionAddress().getAllAddresses()
                    : Arrays.asList(self.getElectionAddress().getOne());

            String addr = addressesToSend.stream()
                    .map(NetUtils::formatInetAddr).collect(Collectors.joining("|"));
            byte[] addr_bytes = addr.getBytes();
            dout.writeInt(addr_bytes.length);
            dout.write(addr_bytes);
            dout.flush();

            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            // 异常情况则关闭Socket通信，一般不会发生
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

        // authenticate learner
        QuorumPeer.QuorumServer qps = self.getVotingView().get(sid);
        if (qps != null) {
            // TODO - investigate why reconfig makes qps null.
            authLearner.authenticate(sock, qps.hostname);
        }

        // If lost the challenge, then drop the new connection
        // 这里是创建集群内通信结构的关键点之一，即在上一篇中写过的complete事件
        // 在本次分析源码的假设三台机器A、B、C中，A的sid最小为1，B的sid居中为3
        // C的sid最大为5，因此在各个机器中，A将会由于sid小于其它的机器而无法主动
        // 建立通信对，B只能主动对A建立通信对，而C可以主动向B和A建立通信对。A和B
        // 的被动连接将会在后续分析Listener类中讲解。
        // 简而言之，sid大的->sid小的=大的建立通信对，sid小的->sid大的=关闭连接
        if (sid > self.getId()) {
            LOG.info("Have smaller server identifier, so dropping the connection: (myId:{} --> sid:{})", self.getId(), sid);
            // 当A->B、A->C和B->C这三种情况时会进入到这里面主动关闭本Socket
            // 解释：sid小的主动连接sid大的会主动关闭Socket连接。显示场景为：
            // B和C机器都已经启动了，而A是最后启动的，此时A机器执行到了这里，
            // A机器会主动的关闭连接
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            LOG.debug("Have larger server identifier, so keeping the connection: (myId:{} --> sid:{})", self.getId(), sid);
            // 当C->A、C->B和B->A这三种情况时会进入到这里面主动创建通信对
            // 解释：sid大的主动连接sid小的将会在本机器中主动创建通信对
            // 根据传入的Socket对象创建通信对，需要注意的是通信对里面的sid是
            // 需要进行通信的机器sid，而不是本机器的
            // 现实场景为：A和B机器已经创建了，C最后启动的，此时C机器由于sid比
            // A和B要大，因此会执行到这里，主动创建和A、B的通信对
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);
            // 获取以前选举通信时可能存在的通信对对象
            SendWorker vsw = senderWorkerMap.get(sid);
            // 如果原来senderWorkerMap中有了sid对应的通信对，则拿出来主动销毁
            // 因为通信对都是线程对象，可能存在以前选举时残留的数据，需要主动的
            // 清空并关闭Socket连接，重新使用新的通信对对象
            if (vsw != null) {
                vsw.finish();
            }
            // 以sid为key，通信对为value放入到senderWorkerMap集合中
            senderWorkerMap.put(sid, sw);
            // 如果消息发送集合中没有key为sid的阻塞队列则先创建放入集合中
            // 再做一次确认，但在刚刚的流程中queueSendMap肯定已经被初始化并
            // 放入了需要发送的数据的
            queueSendMap.putIfAbsent(sid, new CircularBlockingQueue<>(SEND_CAPACITY));
            // 启动发送消息线程对象，开始监听queueSendMap对象的阻塞队列
            sw.start();
            // 启动接收消息线程对象，用来接收对应机器发送来的消息
            rw.start();

            return true;

        }
        return false;
    }

    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     *
     */
    public void receiveConnection(final Socket sock) {
        // 从名字也可以看出来这个方法就是用来接收Socket连接并处理的
        // 和刚刚分析过的initiateConnection方法作用类似，只是
        // initiateConnection方法是让sid大的主动创建通信对，而这个方法
        // 则是让sid小的被动创建通信对
        DataInputStream din = null;
        try {
            // 在上面的initiateConnection方法中说了，在判断sid的大小值并处理
            // 之前，连上Socket的第一件事便是把本机器的myid发送出去。举个例子：
            // A->C，由于A的sid比C的小，因此A不会主动创建和C的通信对，但连接
            // 之后A会立马把自己的myid发送给C，而C->A时C也会主动的把自己的myid
            // 发送给A，从而各自触发Listener监听
            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));

            LOG.debug("Sync handling of connection request received from: {}", sock.getRemoteSocketAddress());
            // todo
            handleConnection(sock, din);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection", sock.getRemoteSocketAddress());
            LOG.debug("Exception details: ", e);
            closeSocket(sock);
        }
    }

    /**
     * Server receives a connection request and handles it asynchronously via
     * separate thread.
     */
    public void receiveConnectionAsync(final Socket sock) {
        try {
            LOG.debug("Async handling of connection request received from: {}", sock.getRemoteSocketAddress());
            connectionExecutor.execute(new QuorumConnectionReceiverThread(sock));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection", sock.getRemoteSocketAddress());
            LOG.debug("Exception details: ", e);
            closeSocket(sock);
        }
    }

    /**
     * Thread to receive connection request from peer server.
     */
    private class QuorumConnectionReceiverThread extends ZooKeeperThread {

        private final Socket sock;
        QuorumConnectionReceiverThread(final Socket sock) {
            super("QuorumConnectionReceiverThread-" + sock.getRemoteSocketAddress());
            this.sock = sock;
        }

        @Override
        public void run() {
            receiveConnection(sock);
        }

    }

    private void handleConnection(Socket sock, DataInputStream din) throws IOException {
        Long sid = null, protocolVersion = null;
        MultipleAddresses electionAddr = null;

        try {
            // 读取其它机器发送过来的myid
            protocolVersion = din.readLong();

            if (protocolVersion >= 0) { // this is a server id and not a protocol version
                sid = protocolVersion;
            } else {
                try {
                    // todo
                    InitialMessage init = InitialMessage.parse(protocolVersion, din);
                    sid = init.sid;
                    if (!init.electionAddr.isEmpty()) {
                        electionAddr = new MultipleAddresses(init.electionAddr,
                                Duration.ofMillis(self.getMultiAddressReachabilityCheckTimeoutMs()));
                    }
                    LOG.debug("Initial message parsed by {}: {}", self.getId(), init.toString());
                } catch (InitialMessage.InitialMessageException ex) {
                    LOG.error("Initial message parsing error!", ex);
                    closeSocket(sock);
                    return;
                }
            }
            // 如果这个sid等于观察者的id，则将其赋值为observerCounter，每次
            // 有新的观察者，observerCounter都会减一，保持sid的特殊性以及
            // 观察者sid的唯一性
            if (sid == QuorumPeer.OBSERVER_ID) {
                /*
                 * Choose identifier at random. We need a value to identify
                 * the connection.
                 */
                sid = observerCounter.getAndDecrement();
                LOG.info("Setting arbitrary identifier to observer: {}", sid);
            }
        } catch (IOException e) {
            LOG.warn("Exception reading or writing challenge", e);
            closeSocket(sock);
            return;
        }

        // do authenticating learner
        authServer.authenticate(sock, din);
        //If wins the challenge, then close the new connection.
        // 看到这里又是熟悉的感觉，在initiateConnection方法中也有类似的场景
        // 但是需要注意的是initiateConnection方法第一个if判断语句条件是
        // “sid > self.getId()”，和本方法中的if判断相反，原因就是本方法实际上
        // 就是initiateConnection方法的被动实现。
        // 依然是A、B、C三台机器，我们已经确认了经过在initiateConnection方法中
        // 执行完后的逻辑，C将会有B和A的通信对，而B将会有A的通信对，所有sid大的
        // 机器都会有sid小的机器通信对，但是小的sid机器没有大的sid机器通信对。
        // 以上述情况是根本无法做到集群内的机器互相通信的，因此需要本方法来补充
        // 下面的逻辑大致为：sid小的可以在本机被动的创建和sid大的机器通信对；而
        // sid大的机器接收到sid小的机器连接请求后，如果本机器没有sid小的机器的
        // 通信对，则会关闭本次的Socket对象并在本机建立和sid小的机器的通信对。
        if (sid < self.getId()) {
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            // 进入到这里的情况是A->B、A->C、B->C，即sid小的机器向sid大的机器
            // 发送请求连接，现实场景可以理解成A机器在C机器后面启动，A机器在启
            // 动的时候向C机器发送连接请求，但由于C没启动，无法达到，因此作废。
            // 而等到A机器启动时就会向C机器发送请求，此时C机器在监听到了A的请求
            // 后便会遍执行到了这里
            // 从本机器的senderWorkerMap集合取出可能存在的通信对
            SendWorker sw = senderWorkerMap.get(sid);
            // 如果原来存在的将原来的通信对销毁释放
            if (sw != null) {
                // 销毁通信对
                sw.finish();
            }

            /*
             * Now we start a new connection
             */
            LOG.debug("Create new connection to server: {}", sid);
            // 关闭A或者B机器（即sid小的机器）的连接请求Socket对象
            closeSocket(sock);
            // 调用已经分析过的connectOne方法，开始在本机器上再次主动创建
            // 和A、B机器的通信对（即sid小的机器）
            if (electionAddr != null) {
                connectOne(sid, electionAddr);
            } else {
                connectOne(sid);
            }

        } else if (sid == self.getId()) {
            // we saw this case in ZOOKEEPER-2164
            LOG.warn("We got a connection request from a server with our own ID. "
                     + "This should be either a configuration error, or a bug.");
        } else { // Otherwise start worker threads to receive data.
            // 进入到这里的情况是C->A、C->B、B->A，即sid大的机器向sid小的机器
            // 发送连接请求，此时sid小的机器监听后将会执行到这里开始在本机器中
            // 被动的创建和sid大的机器的通信对
            // 显示场景为：A和B机器已经启动了，但是C机器最后启动的，此时C机器
            // 会向A和B机器发送连接请求，A和B机器由于sid小于C机器，因此监听到
            // 连接请求后会执行到这里被动的创建和C机器的通信对
            // 使用sid大的机器信息和Socket通信对象创建通信对
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            // 如果本机器原来有sid对应机器的通信对则销毁
            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null) {
                // 调用销毁方法
                vsw.finish();
            }
            // 将新的通信对放入到senderWorkerMap集合中以便通信对可以监听
            // 集合的消息变化
            senderWorkerMap.put(sid, sw);
            // 如果保存要发送消息集合不包含新请求进来的sid对应机器则创建
            queueSendMap.putIfAbsent(sid, new CircularBlockingQueue<>(SEND_CAPACITY));
            // 启动通信对发送消息线程对象，开始监听queueSendMap集合发送消息
            sw.start();
            // 启动通信对接收消息线程对象，开始监听其它机器的Socket消息并接收
            rw.start();
        }
    }

    /**
     * Processes invoke this message to queue a message to send. Currently,
     * only leader election uses it.
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        // 如果要发送的myid等于本机器的id，不用发送，直接放入recvQueue集合中
        // 需要注意的是recvQueue集合和前面在FLE对象中提到的recvqueue集合很像
        // 这里做个简单说明：recvQueue集合是和FLE中的WorkerReceiver进行交互的
        // recvqueue集合则是WorkerReceiver和真正的FLE对象交互的。交互对象需要
        // 搞清楚，要不然看源码的时候很容易迷糊
        if (this.mySid == sid) {
            b.position(0);
            // 直接添加到recvQueue集合中，相当于已经通过RecvWorker收到了消息
            // 但是由于是发给自己的，因此忽略了RecvWorker这一步
            addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * Otherwise send to the corresponding thread to send.
             */
        } else {
            /*
             * Start a new connection if doesn't have one already.
             */
            // 如果集合中还没有sid的阻塞队列，则进行创建并放入到集合中
            BlockingQueue<ByteBuffer> bq = queueSendMap.computeIfAbsent(sid, serverId -> new CircularBlockingQueue<>(SEND_CAPACITY));
            // 再将需要发送的ByteBuffer对象消息放入到阻塞队列中
            addToSendQueue(bq, b);
            // 真正开始根据sid去和对应的机器创建Socket长通信
            connectOne(sid);
        }
    }

    /**
     * Try to establish a connection to server with id sid using its electionAddr.
     * The function will return quickly and the connection will be established asynchronously.
     *
     * VisibleForTesting.
     *
     *  @param sid  server id
     *  @return boolean success indication
     */
    synchronized boolean connectOne(long sid, MultipleAddresses electionAddr) {
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server {}", sid);
            if (self.isMultiAddressEnabled() && electionAddr.size() > 1 && self.isMultiAddressReachabilityCheckEnabled()) {
                // since ZOOKEEPER-3188 we can use multiple election addresses to reach a server. It is possible, that the
                // one we are using is already dead and we need to clean-up, so when we will create a new connection
                // then we will choose an other one, which is actually reachable
                senderWorkerMap.get(sid).asyncValidateIfSocketIsStillReachable();
            }
            return true;
        }

        // we are doing connection initiation always asynchronously, since it is possible that
        // the socket connection timeouts or the SSL handshake takes too long and don't want
        // to keep the rest of the connections to wait
        // 如果和一台机器的myid为sid没有创建过通信对则准备创建
        return initiateConnectionAsync(electionAddr, sid);
    }

    /**
     * Try to establish a connection to server with id sid.
     * The function will return quickly and the connection will be established asynchronously.
     *
     *  @param sid  server id
     */
    synchronized void connectOne(long sid) {
        // 如果和一台机器的myid为sid没有创建过通信对则准备创建
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server {}", sid);
            if (self.isMultiAddressEnabled() && self.isMultiAddressReachabilityCheckEnabled()) {
                // since ZOOKEEPER-3188 we can use multiple election addresses to reach a server. It is possible, that the
                // one we are using is already dead and we need to clean-up, so when we will create a new connection
                // then we will choose an other one, which is actually reachable
                senderWorkerMap.get(sid).asyncValidateIfSocketIsStillReachable();
            }
            return;
        }

        synchronized (self.QV_LOCK) {
            boolean knownId = false;
            // Resolve hostname for the remote server before attempting to
            // connect in case the underlying ip address has changed.
            self.recreateSocketAddresses(sid);
            Map<Long, QuorumPeer.QuorumServer> lastCommittedView = self.getView();
            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            Map<Long, QuorumPeer.QuorumServer> lastProposedView = lastSeenQV.getAllMembers();
            if (lastCommittedView.containsKey(sid)) {
                knownId = true;
                LOG.debug("Server {} knows {} already, it is in the lastCommittedView", self.getId(), sid);
                if (connectOne(sid, lastCommittedView.get(sid).electionAddr)) {
                    return;
                }
            }
            if (lastSeenQV != null
                && lastProposedView.containsKey(sid)
                && (!knownId
                    || !lastProposedView.get(sid).electionAddr.equals(lastCommittedView.get(sid).electionAddr))) {
                knownId = true;
                LOG.debug("Server {} knows {} already, it is in the lastProposedView", self.getId(), sid);

                if (connectOne(sid, lastProposedView.get(sid).electionAddr)) {
                    return;
                }
            }
            if (!knownId) {
                LOG.warn("Invalid server id: {} ", sid);
            }
        }
    }

    /**
     * Try to establish a connection with each server if one
     * doesn't exist.
     */

    public void connectAll() {
        long sid;
        for (Enumeration<Long> en = queueSendMap.keys(); en.hasMoreElements(); ) {
            sid = en.nextElement();
            connectOne(sid);
        }
    }

    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    boolean haveDelivered() {
        // queueSendMap的key为其它server的id，value为一个队列。
        // 这个队列中存放的是当前server向这个server发送失败的消息。
        for (BlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            final int queueSize = queue.size();
            LOG.debug("Queue size: {}", queueSize);
            // 若queueSize为0，说明当前server向某个其它server发送的消息全部成功了，
            // 即当前server与整个集群没有失联，其消息交付是成功的
            if (queueSize == 0) {
                return true;
            }
        }
        // 代码走到这里，说明所有queue的size都不是0，即当前server向所有其它server
        // 发送的消息全部失败，也就是说，当前server与整个集群失联了
        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();

        // Wait for the listener to terminate.
        try {
            listener.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted before joining the listener", ex);
        }
        softHalt();

        // clear data structures used for auth
        if (connectionExecutor != null) {
            connectionExecutor.shutdown();
        }
        inprogressConnections.clear();
        resetConnectionThreadCount();
    }

    /**
     * A soft halt simply finishes workers.
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Server {} is soft-halting sender towards: {}", self.getId(), sw);
            sw.finish();
        }
    }

    /**
     * Helper method to set socket options.
     *
     * @param sock
     *            Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setKeepAlive(tcpKeepAlive);
        sock.setSoTimeout(this.socketTimeout);
    }

    /**
     * Helper method to close a socket.
     *
     * @param sock
     *            Reference to socket
     */
    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }

    /**
     * Return number of connection processing threads.
     */
    public long getConnectionThreadCount() {
        return connectionThreadCnt.get();
    }

    /**
     * Reset the value of connection processing threads count to zero.
     */
    private void resetConnectionThreadCount() {
        connectionThreadCnt.set(0);
    }

    /**
     * Thread to listen on some ports
     */
    public class Listener extends ZooKeeperThread {

        private static final String ELECTION_PORT_BIND_RETRY = "zookeeper.electionPortBindRetry";
        private static final int DEFAULT_PORT_BIND_MAX_RETRY = 3;

        private final int portBindMaxRetry;
        private Runnable socketBindErrorHandler = () -> ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_BIND_QUORUM_PORT.getValue());
        private List<ListenerHandler> listenerHandlers;
        private final AtomicBoolean socketException;


        public Listener() {
            // During startup of thread, thread name will be overridden to
            // specific election address
            super("ListenerThread");

            socketException = new AtomicBoolean(false);

            // maximum retry count while trying to bind to election port
            // see ZOOKEEPER-3320 for more details
            final Integer maxRetry = Integer.getInteger(ELECTION_PORT_BIND_RETRY,
                    DEFAULT_PORT_BIND_MAX_RETRY);
            if (maxRetry >= 0) {
                LOG.info("Election port bind maximum retries is {}", maxRetry == 0 ? "infinite" : maxRetry);
                portBindMaxRetry = maxRetry;
            } else {
                LOG.info(
                  "'{}' contains invalid value: {}(must be >= 0). Use default value of {} instead.",
                  ELECTION_PORT_BIND_RETRY,
                  maxRetry,
                  DEFAULT_PORT_BIND_MAX_RETRY);
                portBindMaxRetry = DEFAULT_PORT_BIND_MAX_RETRY;
            }
        }

        /**
         * Change socket bind error handler. Used for testing.
         */
        void setSocketBindErrorHandler(Runnable errorHandler) {
            this.socketBindErrorHandler = errorHandler;
        }

        @Override
        public void run() {
            if (!shutdown) {
                LOG.debug("Listener thread started, myId: {}", self.getId());
                Set<InetSocketAddress> addresses;

                // 获取本机器在配置中所配置的端口或者地址
                if (self.getQuorumListenOnAllIPs()) {
                    addresses = self.getElectionAddress().getWildcardAddresses();
                } else {
                    addresses = self.getElectionAddress().getAllAddresses();
                }

                CountDownLatch latch = new CountDownLatch(addresses.size());
                listenerHandlers = addresses.stream().map(address ->
                                new ListenerHandler(address, self.shouldUsePortUnification(), self.isSslQuorum(), latch))
                        .collect(Collectors.toList());

                final ExecutorService executor = Executors.newFixedThreadPool(addresses.size());
                try {
                    listenerHandlers.forEach(executor::submit);
                } finally {
                    // prevent executor's threads to leak after ListenerHandler tasks complete
                    executor.shutdown();
                }

                try {
                    latch.await();
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while sleeping. Ignoring exception", ie);
                } finally {
                    // Clean up for shutdown.
                    for (ListenerHandler handler : listenerHandlers) {
                        try {
                            handler.close();
                        } catch (IOException ie) {
                            // Don't log an error for shutdown.
                            LOG.debug("Error closing server socket", ie);
                        }
                    }
                }
            }

            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error(
                  "As I'm leaving the listener thread, I won't be able to participate in leader election any longer: {}",
                  self.getElectionAddress().getAllAddresses().stream()
                    .map(NetUtils::formatInetAddr)
                    .collect(Collectors.joining("|")));
                if (socketException.get()) {
                    // After leaving listener thread, the host cannot join the quorum anymore,
                    // this is a severe error that we cannot recover from, so we need to exit
                    socketBindErrorHandler.run();
                }
            }
        }

        /**
         * Halts this listener thread.
         */
        void halt() {
            LOG.debug("Halt called: Trying to close listeners");
            if (listenerHandlers != null) {
                LOG.debug("Closing listener: {}", QuorumCnxManager.this.mySid);
                for (ListenerHandler handler : listenerHandlers) {
                    try {
                        handler.close();
                    } catch (IOException e) {
                        LOG.warn("Exception when shutting down listener: ", e);
                    }
                }
            }
        }

        class ListenerHandler implements Runnable, Closeable {
            private ServerSocket serverSocket;
            private InetSocketAddress address;
            private boolean portUnification;
            private boolean sslQuorum;
            private CountDownLatch latch;

            ListenerHandler(InetSocketAddress address, boolean portUnification, boolean sslQuorum,
                            CountDownLatch latch) {
                this.address = address;
                this.portUnification = portUnification;
                this.sslQuorum = sslQuorum;
                this.latch = latch;
            }

            /**
             * Sleeps on acceptConnections().
             */
            @Override
            public void run() {
                try {
                    // 设置本listener的地址名称
                    Thread.currentThread().setName("ListenerHandler-" + address);
                    acceptConnections();
                    try {
                        close();
                    } catch (IOException e) {
                        LOG.warn("Exception when shutting down listener: ", e);
                    }
                } catch (Exception e) {
                    // Output of unexpected exception, should never happen
                    LOG.error("Unexpected error ", e);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public synchronized void close() throws IOException {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    LOG.debug("Trying to close listeners: {}", serverSocket);
                    serverSocket.close();
                }
            }

            /**
             * Sleeps on accept().
             */
            private void acceptConnections() {
                int numRetries = 0;
                Socket client = null;
                // IO失败可重试portBindMaxRetry 3次
                while ((!shutdown) && (portBindMaxRetry == 0 || numRetries < portBindMaxRetry)) {
                    try {
                        serverSocket = createNewServerSocket();
                        LOG.info("{} is accepting connections now, my election bind port: {}", QuorumCnxManager.this.mySid, address.toString());
                        while (!shutdown) {
                            try {
                                // 开始接收其它机器发送过来的连接请求，sid大的或者sid小的
                                // 都会发送连接请求，在前面分析过，sid小的对sid大的机器发
                                // 送连接之后会主动关闭连接，其对sid大的机器创建通信对的操
                                // 作便是放在这个流程中
                                client = serverSocket.accept();
                                setSockOpts(client);
                                LOG.info("Received connection request from {}", client.getRemoteSocketAddress());
                                // Receive and handle the connection request
                                // asynchronously if the quorum sasl authentication is
                                // enabled. This is required because sasl server
                                // authentication process may take few seconds to finish,
                                // this may delay next peer connection requests.
                                // 接收到其它机器的请求后开始处理
                                if (quorumSaslAuthEnabled) {
                                    receiveConnectionAsync(client);
                                } else {
                                    receiveConnection(client);
                                }
                                // 重试次数重置为0
                                numRetries = 0;
                            } catch (SocketTimeoutException e) {
                                LOG.warn("The socket is listening for the election accepted "
                                        + "and it timed out unexpectedly, but will retry."
                                        + "see ZOOKEEPER-2836");
                            }
                        }
                    } catch (IOException e) {
                        if (shutdown) {
                            break;
                        }

                        LOG.error("Exception while listening to address {}", address, e);

                        if (e instanceof SocketException) {
                            socketException.set(true);
                        }

                        numRetries++;
                        try {
                            close();
                            Thread.sleep(1000);
                        } catch (IOException ie) {
                            LOG.error("Error closing server socket", ie);
                        } catch (InterruptedException ie) {
                            LOG.error("Interrupted while sleeping. Ignoring exception", ie);
                        }
                        closeSocket(client);
                    }
                }
                if (!shutdown) {
                    LOG.error(
                      "Leaving listener thread for address {} after {} errors. Use {} property to increase retry count.",
                      formatInetAddr(address),
                      numRetries,
                      ELECTION_PORT_BIND_RETRY);
                }
            }

            private ServerSocket createNewServerSocket() throws IOException {
                ServerSocket socket;

                if (portUnification) {
                    LOG.info("Creating TLS-enabled quorum server socket");
                    socket = new UnifiedServerSocket(self.getX509Util(), true);
                } else if (sslQuorum) {
                    LOG.info("Creating TLS-only quorum server socket");
                    socket = new UnifiedServerSocket(self.getX509Util(), false);
                } else {
                    socket = new ServerSocket();
                }

                socket.setReuseAddress(true);
                address = new InetSocketAddress(address.getHostString(), address.getPort());
                socket.bind(address);

                return socket;
            }
        }

    }

    /**
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     */
    class SendWorker extends ZooKeeperThread {

        // 本个通信对的发送线程对象需要对接通信的机器sid（即对应机器的myid）
        Long sid;
        // 本个通信对的发送线程对象和需要通信机器建立的Socket长连接
        Socket sock;
        // 本通信对的发送消息线程对象对应的接收消息线程对象
        RecvWorker recvWorker;
        // 运行状态
        volatile boolean running = true;
        // 使用Socket对象的outputStream对象流创建的数据输出流对象，负责实际的通信
        DataOutputStream dout;

        AtomicBoolean ongoingAsyncValidation = new AtomicBoolean(false);

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         *
         * @param sock
         *            Socket to remote peer
         * @param sid
         *            Server identifier of remote peer
         */
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: {}", this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         *
         * @return RecvWorker
         */
        synchronized RecvWorker getRecvWorker() {
            return recvWorker;
        }

        synchronized boolean finish() {
            LOG.debug("Calling SendWorker.finish for {}", sid);

            if (!running) {
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }

            running = false;
            closeSocket(sock);

            this.interrupt();
            if (recvWorker != null) {
                recvWorker.finish();
            }

            LOG.debug("Removing entry from senderWorkerMap sid={}", sid);

            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }

        synchronized void send(ByteBuffer b) throws IOException {
            // 为需要发送的字节数组创建新的等长度数组以方便后续进行消息校验
            byte[] msgBytes = new byte[b.capacity()];
            try {
                // 将需要发送的消息数组位置归位，以确保可以校验整个数组
                b.position(0);
                // 调用get方法有两个目的：1、检查数组数据边界是否正常；
                // 2、检查缓存对象中的数据是否超出缓存初始化的大小，如果超出抛异常
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                // 如果缓存数据超出缓存对象的申请大小则说明内存溢出，无法进行正常操作
                return;
            }
            // 校验通过发送缓存对象中的数据，首先发送数据大小，其次再发送整体数据
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        @Override
        public void run() {
            // 有一个线程已经执行，线程数量+1
            threadCnt.incrementAndGet();
            try {
                /**
                 * If there is nothing in the queue to send, then we
                 * send the lastMessage to ensure that the last message
                 * was received by the peer. The message could be dropped
                 * in case self or the peer shutdown their connection
                 * (and exit the thread) prior to reading/processing
                 * the last message. Duplicate messages are handled correctly
                 * by the peer.
                 *
                 * If the send queue is non-empty, then we have a recent
                 * message than that stored in lastMessage. To avoid sending
                 * stale message, we should send the message in the send queue.
                 */
                // 刚启动的时候便去queueSendMap集合中查询是否有本通信对的发送目标
                // 机器消息
                BlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                // 如果没有消息需要发送给目标机器则获取最后一次发送给这个机器的消息
                // 并发送给目标机器
                if (bq == null || isSendQueueEmpty(bq)) {
                    // 获取最后发送给目标机器的消息
                    ByteBuffer b = lastMessageSent.get(sid);
                    // 如果以前发送过消息则调用send()方法发送消息
                    if (b != null) {
                        LOG.debug("Attempting to send lastMessage to sid={}", sid);
                        // 发送消息，具体方法后面再分析
                        send(b);
                    }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                // 发生了意外则销毁本通信对
                this.finish();
            }
            LOG.debug("SendWorker thread started towards {}. myId: {}", sid, QuorumCnxManager.this.mySid);

            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        // 查询queueSendMap集合中是否有本通信对的发送目标机器消息
                        BlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                        // 如果目标机器的阻塞队列不为空则从阻塞队列中获取需要发送的
                        // 消息
                        if (bq != null) {
                            // 如果队列不为空则从阻塞队列中获取数据
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);
                        } else {
                            LOG.error("No queue of incoming messages for server {}", sid);
                            // 如果阻塞队列为空说明初始化有异常，阻塞队列在实例化
                            // 线程对象时就已经被创建，且容量只有1
                            break;
                        }
                        // 从消息阻塞队列中获取到了消息且不为空则进行发送操作，为空
                        // 则继续下一次轮询查询阻塞队列是否有需要发送的消息
                        if (b != null) {
                            // 发送前记录给目标机器发送的最后一次消息对象，以方便
                            // 下次和目标机器通信时的通信对可以继续上次的消息开始
                            // 发送，确保消息的连续性。不用担心如果目标机器接收到
                            // 相同的消息会怎么办，接收方就算接收到了相同的消息
                            // 也不会对结果有什么影响
                            lastMessageSent.put(sid, b);
                            // 调用方法方法去发送消息对象
                            send(b);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue", e);
                    }
                }
            } catch (Exception e) {
                LOG.warn(
                    "Exception when using channel: for id {} my id = {}",
                    sid ,
                    QuorumCnxManager.this.mySid,
                    e);
            }
            // 运行完成说明该通信对已经需要退出，调用销毁方法
            this.finish();

            LOG.warn("Send worker leaving thread id {} my id = {}", sid, self.getId());
        }


        public void asyncValidateIfSocketIsStillReachable() {
            if (ongoingAsyncValidation.compareAndSet(false, true)) {
                new Thread(() -> {
                    LOG.debug("validate if destination address is reachable for sid {}", sid);
                    if (sock != null) {
                        InetAddress address = sock.getInetAddress();
                        try {
                            if (address.isReachable(500)) {
                                LOG.debug("destination address {} is reachable for sid {}", address.toString(), sid);
                                ongoingAsyncValidation.set(false);
                                return;
                            }
                        } catch (NullPointerException | IOException ignored) {
                        }
                        LOG.warn(
                          "destination address {} not reachable anymore, shutting down the SendWorker for sid {}",
                          address.toString(),
                          sid);
                        this.finish();
                    }
                }).start();
            } else {
                LOG.debug("validation of destination address for sid {} is skipped (it is already running)", sid);
            }
        }

    }

    /**
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    class RecvWorker extends ZooKeeperThread {
        // 本个通信对的发送线程对象需要对接通信的机器sid（即对应机器的myid）
        Long sid;
        // 本个通信对的发送线程对象和需要通信机器建立的Socket长连接
        Socket sock;
        // 运行状态
        volatile boolean running = true;
        // 使用Socket对象的outputStream对象流创建的数据输入流对象，负责实际的接收
        // 消息通信
        final DataInputStream din;
        // 本通信对的发送消息线程对象对应的接收消息线程对象，该对象仅在需要销毁
        // 本线程对象时用到finish()方法
        final SendWorker sw;

        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for {}", sid, e);
                closeSocket(sock);
                running = false;
            }
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            LOG.debug("RecvWorker.finish called. sid: {}. myId: {}", sid, QuorumCnxManager.this.mySid);
            if (!running) {
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }
            running = false;

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            // 有一个线程已经执行，线程数量+1
            threadCnt.incrementAndGet();
            try {
                LOG.debug("RecvWorker thread towards {} started. myId: {}", sid, QuorumCnxManager.this.mySid);
                // 开始轮询din对象接收sid目标机器的消息
                while (running && !shutdown && sock != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    // 接收消息需要和发送方一样，发送方在发送消息时第一步便把
                    // 消息的长度发送了过来，因此接收也是首先接收数据长度
                    int length = din.readInt();
                    // 如果数据长度不符合则会抛出异常退出，因此发送方才会进行必要的
                    // 校验，因为接收方接收到不符合规范的之后将会关闭连接通信
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException("Received packet with invalid packet: " + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    // 从目标机器的通信Socket对象接收完整的数据
                    final byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    // 封装生成对应的缓存对象ByteBuffer
                    // 将接收到的数据添加到recvQueue集合中，recvQueue集合为
                    // QuorumCnxManager对象和FLE选举算法对象进行消息交互的集合
                    addToRecvQueue(new Message(ByteBuffer.wrap(msgArray), sid));
                }
            } catch (Exception e) {
                LOG.warn(
                    "Connection broken for id {}, my id = {}",
                    sid,
                    QuorumCnxManager.this.mySid,
                    e);
            } finally {
                LOG.warn("Interrupting SendWorker thread from RecvWorker. sid: {}. myId: {}", sid, QuorumCnxManager.this.mySid);
                // 抛出了IO异常之后销毁本通信对并关闭Socket连接对象
                sw.finish();
                // 关闭Socket连接对象
                closeSocket(sock);
            }
        }

    }

    /**
     * Inserts an element in the provided {@link BlockingQueue}. This method
     * assumes that if the Queue is full, an element from the head of the Queue is
     * removed and the new item is inserted at the tail of the queue. This is done
     * to prevent a thread from blocking while inserting an element in the queue.
     *
     * @param queue Reference to the Queue
     * @param buffer Reference to the buffer to be inserted in the queue
     */
    private void addToSendQueue(final BlockingQueue<ByteBuffer> queue,
        final ByteBuffer buffer) {
        final boolean success = queue.offer(buffer);
        if (!success) {
          throw new RuntimeException("Could not insert into receive queue");
        }
    }

    /**
     * Returns true if queue is empty.
     * @param queue
     *          Reference to the queue
     * @return
     *      true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(final BlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link BlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    private ByteBuffer pollSendQueue(final BlockingQueue<ByteBuffer> queue,
          final long timeout, final TimeUnit unit) throws InterruptedException {
       return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts the
     * element at the tail of the queue.
     *
     * @param msg Reference to the message to be inserted in the queue
     */
    public void addToRecvQueue(final Message msg) {
        // 将最新需要发送的消息添加到集合中
      final boolean success = this.recvQueue.offer(msg);
      if (!success) {
          throw new RuntimeException("Could not insert into receive queue");
      }
    }

    /**
     * Retrieves and removes a message at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link BlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    public Message pollRecvQueue(final long timeout, final TimeUnit unit)
       throws InterruptedException {
       return this.recvQueue.poll(timeout, unit);
    }

    public boolean connectedToPeer(long peerSid) {
        return senderWorkerMap.get(peerSid) != null;
    }

    public boolean isReconfigEnabled() {
        return self.isReconfigEnabled();
    }

}
