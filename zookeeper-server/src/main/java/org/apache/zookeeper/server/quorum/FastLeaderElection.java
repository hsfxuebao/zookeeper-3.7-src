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
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumOracleMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * 翻译：使用TCP实现了Leader的选举。它使用QuorumCnxManager类的对象进行连接管理
 *   (与其它Server间的连接管理)。否则(即若不使用QuorumCnxManager对象的话)，将使用
 *   UDP的基于推送的算法实现。
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 * 翻译：有几个参数可以用来改变它(选举)的行为。第一，finalizeWait(这是一个代码中的常量)
 *  决定了选举出一个Leader的时间，这是Leader选举算法的一部分。
 */

public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    /**
     * 翻译：完成等待时间，一旦它(这个过程)认为它已经到达了选举的最后。
     * (该常量)确定还需要等待多长时间
     *
     * 解释：
     * finalizeWait：选举时发送投票给其他服务器，并接受回复，接受到所有回复用到到最长时间就是200毫秒
     * 因为一般200毫秒之内选举已经结束了，所以一般都低于200毫秒
     */
    static final int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */
    /**
     * 翻译：(该常量指定了)两个连续的notification检查间的时间间隔上限。
     * 它影响了系统在经历了长时间分割后再次重启的时间。目前60秒。
     *
     * 该常量是前面的finalizeWait所描述的超时时限的最大值
     *
     * 解释：
     * finalizeWait这个值会增大，直到增大到maxNotificationInterval，一但到达maxNotificationInterval，
     * 还没有选举出来（长时间分割意思就是没有leader），就会重启机器，相当于重新再进行一次新的选举
     */
    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL = "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL = "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        LOG.info("{} = {} ms", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
        LOG.info("{} = {} ms", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    /**
     * 翻译：连接管理者。FastLeaderElection(选举算法)使用TCP(管理)
     * 两个同辈Server的通信，并且QuorumCnxManager还管理着这些连接。
     *
     * 解释：通过该类管理和其他Server之间的TCP连接
     */
    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */
    /**
     * 翻译：Notifications是一个让其它Server知道当前Server已经改变
     * 了投票的通知消息，(为什么它要改变投票呢？)要么是因为它参与了
     * Leader选举(新一轮的投票，首先投向自己)，要么是它知道了另一个
     * Server具有更大的zxid，或者zxid相同但ServerId更大（所以它要
     * 通知给其它所有Server，它要修改自己的选票）。
     */
    public static class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public static final int CURRENTVERSION = 0x2;
        int version;
        /*
         * 它是一个内部类，里面封装了：
         * leader：proposed leader，当前通知所推荐的leader的serverId
         * zxid：当前通知所推荐的leader的zxid
         * electionEpoch：当前通知所处的选举epoch，即逻辑时钟
         * state：QuorumPeer.ServerState，当前通知发送者的状态
         * 		四个状态：LOOKING, FOLLOWING, LEADING, OBSERVING;
         * sid：当前发送者的ServerId
         * peerEpoch： 当前通知所推荐的leader的epoch
         */

        /*
         * Proposed leader
         */ long leader;

        /*
         * zxid of the proposed leader
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * current state of sender
         */ QuorumPeer.ServerState state;

        /*
         * Address of sender
         */ long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */ long peerEpoch;

    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    /**
     * Notification是作为本地处理通知消息时封装的对象
     * 如果要将通知发送出去或者接收，用的是ToSend对象，其成员变量和Notification一致
     */
    public static class ToSend {

        enum mType {
            crequest,
            challenge,
            notification,
            ack
        }

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch, byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */ long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * Current state;
         */ QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */ long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData;

        /*
         * Leader epoch
         */ long peerEpoch;

    }

    /**
     * 选举时发送和接收选票通知都是异步的，先放入队列，有专门线程处理
     * 下面两个就是发送消息队列，和接收消息的队列
     */
    // 将要使用通信对发送消息的消息存储队列集合，通信对发送消息时将会从该集合中
    // 取出消息对象并使用Socket通信发送给对应的机器
    LinkedBlockingQueue<ToSend> sendqueue;
    // WorkerReceiver线程对象和实际的FLE选举算法对象进行通信的集合，也就是说
    // FLE对象需要和QuorumCnxManager对象进行交互中间需要经过两次集合传递
    // 即：QuorumCnxManager->recvQueue->WorkerReceiver->recvqueue->FLE
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */
    /**
     * 翻译：消息处理程序的多线程实现。Messenger实现了两个子类:WorkReceiver和WorkSender。
     * 从名称可以明显看出它们的功能。每一个都生成一个新线程。
     *
     * 解释：通过创建该类可以创建两个线程分别处理消息发送和接收
     */
    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
        /**
         * 翻译：接收QuorumCnxManager实例在方法run()上的消息，并处理这些消息。
         */
        //大概逻辑就是通过QuorumCnxManager manager，获取其他Server发来的消息，然后处理，如果
        //符合一定条件，放到recvqueue队列里
        class WorkerReceiver extends ZooKeeperThread {

            // 运行状态
            volatile boolean stop;
            // 与其进行交互的集群连接管理对象
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        // 从连接管理对象的recvQueue集合中取出通信对RecvWorker对象
                        // 接收并放入的消息对象
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        // 如果经过了3s还是没查询到消息对象则继续下次轮询
                        if (response == null) {
                            continue;
                        }

                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        // 如果接收到的消息小于28长度，说明可能是简单的响应或者
                        // 不能影响到实际选举流程的消息，退出本次轮询开始下次
                        if (capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        // 用来兼容某些消息未发送sid机器的peerEpoch值
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);
                        // 让response的位置属性变成初始化状态（但数据并未删除）
                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        // 创建通知对象
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;

                        try {
                            // 如果消息长度不为28说明显式的把peerEpoch值传了过来
                            // 否则需要从发送过来的zxid中获取对应的peerEpoch值
                            if (!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong();
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */
                                    // 获取版本信息，3.4.6新增的
                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }

                            // check if we have a version that includes config. If so extract config info from message.
                            if (version > 0x1) {
                                int configLength = response.buffer.getInt();

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                                                        response.sid, capacity, version, configLength));
                                }

                                byte[] b = new byte[configLength];
                                response.buffer.get(b);

                                synchronized (self) {
                                    try {
                                        rqv = self.configFromString(new String(b, UTF_8));
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if (rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}",
                                                     self.getId(),
                                                     Long.toHexString(rqv.getVersion()),
                                                     Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                     response.sid, capacity, e);
                            continue;
                        }
                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(
                                ToSend.mType.notification,
                                current.getId(),
                                current.getZxid(),
                                logicalclock.get(),
                                self.getPeerState(),
                                response.sid,
                                current.getPeerEpoch(),
                                qv.toString().getBytes(UTF_8));

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            LOG.debug("Receive new notification message. My id = {}", self.getId());

                            // State of peer that sent this message
                            // 默认状态为LOOKING，这样即使消息异常也不会造成实际的影响
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            // 从接收到的消息中取出发送消息机器的状态并在后续进行
                            // 相应的赋值buffer中的值信息可以在上一篇的WorkerSender
                            // 线程对象buildMsg()方法分析中对应上，当然也可以不用管
                            // 只需要知道这种获取顺序是可以准确的拿到发送消息数据即可
                            switch (rstate) {
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }

                            // 从消息对象中分别获取对应的数据，需要注意的是这些数据
                            // 并不是sid机器的，而是sid机器认为是Leader机器的
                            // 打个比方：A机器现在接收到B机器发送过来的消息，B机器认为
                            // C机器是集群的Leadere，机器A接收到的response的值便是C
                            // 机器的而不是B机器的
                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            LOG.info(
                                "Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, "
                                    + "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                                self.getPeerState(),
                                n.sid,
                                n.state,
                                n.leader,
                                Long.toHexString(n.electionEpoch),
                                Long.toHexString(n.peerEpoch),
                                Long.toHexString(n.zxid),
                                Long.toHexString(n.version),
                                (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            // 如果接收到的消息状态为LOOKING选举状态，则说明发送消息
                            // 的机器处于选举状态，启动时基本所有参与选举的机器都会
                            // 进入到这个判断中，算是选举状态的普遍性条件
                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                // 接收到消息后将其添加到recvqueue集合中，该集合就是
                                // 本对象和FLE选举对象交互的集合
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                // 这个条件判断是为了中途退出了集群后来又连接上来的
                                // 机器消息使用，消息通知的electionEpoch属性就是
                                // 机器sid上的logicalclock，因此这个条件可以理解成：
                                // 如果发送消息机器的logicalclock选举轮次要比本集群
                                // 中的选举轮次要低，则直接把本机器认为的可能是Leader
                                // 的机器信息发送给机器sid，让其跟着本集群选举走
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                    && (n.electionEpoch < logicalclock.get())) {
                                    // 获取当前机器认为的Leader机器信息
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    // 转化化成待发送消息对象
                                    ToSend notmsg = new ToSend(
                                        ToSend.mType.notification,
                                        v.getId(),
                                        v.getZxid(),
                                        logicalclock.get(),
                                        self.getPeerState(),
                                        response.sid,
                                        v.getPeerEpoch(),
                                        qv.toString().getBytes());
                                    // 放入sendqueue集合中以便通信对的发送对象获取
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                // 进入到这里说明本集群已经产生了Leader，而接受到的
                                // 选举消息大概率是原来在集群中，但是由于网络或者其它
                                // 原因导致中途退出了，而现在中途再次加入到集群中
                                // 先获取本集群的Leader投票信息
                                Vote current = self.getCurrentVote();
                                // 如果发送消息过来的机器处于选举状态，即原来的投票
                                // 信息已经失效，需要再次投票以加入到本集群中来
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (self.leader != null) {
                                        if (leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        self.leader.reportLookingSid(response.sid);
                                    }


                                    LOG.debug(
                                        "Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                        self.getId(),
                                        response.sid,
                                        Long.toHexString(current.getZxid()),
                                        current.getId(),
                                        Long.toHexString(self.getQuorumVerifier().getVersion()));

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    // 直接使用本集群内的Leader
                                    // 信息封装成待发送消息
                                    ToSend notmsg = new ToSend(
                                        ToSend.mType.notification,
                                        current.getId(),
                                        current.getZxid(),
                                        current.getElectionEpoch(),
                                        self.getPeerState(),
                                        response.sid,
                                        current.getPeerEpoch(),
                                        qv.toString().getBytes());
                                    // 添加到待发送集合中以便SendWorker通信对发送
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message", e);
                    }
                }
                LOG.info("WorkerReceiver is down");
            }

        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        /**
         * 翻译：这个worker只是取出要发送的消息并将其放入管理器的队列中。
         */
        //大概逻辑就是从sendqueue队列中获取要发送的消息ToSend对象，调用上面说过的buildMsg方法
        //转换成字节数据，再通过QuorumCnxManager manager广播出去
        class WorkerSender extends ZooKeeperThread {

            volatile boolean stop;
            // 集群连接管理对象，WorkerSender实际上是该对象的内部类
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                // 启动通信对的发送信息对象后本方法将会被执行，直到该对象被调用finish()
                // 方法销毁
                while (!stop) {
                    try {
                        // 从消息队列集合中获取需要发送的消息对象，固定阻塞3s，如果
                        // 没有轮询到则返回null
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        // 如果为null说明暂时没有消息发送，继续轮回
                        if (m == null) {
                            continue;
                        }
                        // 如果不为空则说明有需要发送的消息，调用process发送消息对象
                        process(m);
                    } catch (InterruptedException e) {
                        // 如果轮询集合发生异常则退出
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                // 将需要发送的消息转换成Socket方便发送的ByteBuffer缓存对象
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);
                // 通知连接管理对象需要发送requestBuffer对象中的信息
                manager.toSend(m.sid, requestBuffer);

            }

        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         * 通过构造Messenger对象，即可启动两个线程处理收发消息
         */
        Messenger(QuorumCnxManager manager) {
            // 创建一个WorkerSender线程
            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         * 调用该方法，让收发消息两个线程停止工作
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    // 本机器的集群对象 可以简单理解为：代表当前参与选举的Server
    QuorumPeer self;

    Messenger messenger;
    // 选举流程时的逻辑迭代数，每调用一次lookForLeader进行选举时该值会+1
    // 发送到其它机器上时对应Notification对象的electionEpoch属性
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    // 本机推崇将要当选leader的myid，对应Notification对象的leader，可以看成是
    // 某个机器的id
    long proposedLeader;
    // 本机推崇将要当选leader的zxid，对应Notification对象的zxid
    long proposedZxid;
    // 本机推崇将要当选leader的epoch，对应Notification对象的peerEpoch
    long proposedEpoch;

    /**
     * Returns the current value of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    /**
     * 发送投票通知的时候，将ToSend封装的选票信息转换成二进制字节数据传输
     */
    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        // 生成ByteBuffer对象并封装byte[]数组，这里需要特别说明下各个参数和
        // 在FLE中参数的对应关系
        byte[] requestBytes = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         * 注意，是按照一定顺序转换的，接收的时候也按照该顺序解析
         */
        requestBuffer.clear();
        // 对应PeerQuorum中的peerState，此时值为LOOKING
        requestBuffer.putInt(state);
        // 对应PeerQuorum中的proposedLeader，刚开始选举为本机器的myid
        requestBuffer.putLong(leader);
        // 对应PeerQuorum中的proposedZxid，刚开始选举为本机器的zxid
        requestBuffer.putLong(zxid);
        // 对应PeerQuorum中的logicclock，代表本次选举的迭代数
        requestBuffer.putLong(electionEpoch);
        // 对应PeerQuorum中的proposedEpoch，选举开始为本机器的currentEpoch
        requestBuffer.putLong(epoch);
        // 默认版本信息，接收到后会设置为接收消息的version属性
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    /**
     * 翻译：FastLeaderElection的构造函数。它接受两个参数，
     * 一个是实例化此对象的QuorumPeer对象，另一个是连接管理器。
     * 在ZooKeeper服务的一个实例期间，每个对等点只应该创建这样的对象一次。
     */
    // 一个zk server只会创建一个FastLeaderElection实例
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        //starter：启动选举，初始化一些数据、启动收发消息的线程等操作..
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;
        // 创建了两个队列
        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        // 创建了一个管理者，其可以将通知发送给其它server
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        LOG.debug(
            "About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
            v.getId(),
            Long.toHexString(v.getZxid()),
            self.getId(),
            self.getPeerState());
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;
    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        // 轮询配置文件中所配置的各个Server信息，并向每台机器发送通知
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            // 将本机器的信息封装，并发给myid为sid的机器
            ToSend notmsg = new ToSend(
                ToSend.mType.notification,
                proposedLeader, // 第一次发送此值为本机器的myid
                proposedZxid, // 第一次发送此值为本机器的zxid
                logicalclock.get(), // 第一次发送此值为本机器的logicalclock
                QuorumPeer.ServerState.LOOKING, // 本机器流程为LOOKING
                sid, // 目标机器的myid
                proposedEpoch, // 第一次发送此值为本机器的currentEpoch
                qv.toString().getBytes(UTF_8));

            LOG.debug(
                "Sending Notification: {} (n.leader), 0x{} (n.zxid), 0x{} (n.round), {} (recipient),"
                    + " {} (myid), 0x{} (n.peerEpoch) ",
                proposedLeader,
                Long.toHexString(proposedZxid),
                Long.toHexString(logicalclock.get()),
                sid,
                self.getId(),
                Long.toHexString(proposedEpoch));
            // 放入sendqueue集合中以便本选举对象的WorkerSender发送这些
            // 通知消息给其它的机器
            sendqueue.offer(notmsg);
        }
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug(
            "id: {}, proposed id: {}, zxid: 0x{}, proposed zxid: 0x{}",
            newId,
            curId,
            Long.toHexString(newZxid),
            Long.toHexString(curZxid));
        // 一个server的权重为0，则其为Observer，是不能做leader的
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        return ((newEpoch > curEpoch)
                || ((newEpoch == curEpoch)
                    && ((newZxid > curZxid)
                        || ((newZxid == curZxid)
                            && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        // 创建一个跟踪器
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        // 以下都是用于初始化这个跟踪器
        // 将当前的QuorumVerifier添加到跟踪器，只不过此时该QuorumVerifier对应的ackset还为空
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        // 若存在更新版本的QuorumVerifier，则将这个QuorumVerifier放入跟踪器
        // 由于跟踪器是每当票箱发生变更就会创建一个新的，所以可以推断，这个跟踪器中
        // 最多包含两个QuorumVerifier
        if (self.getLastSeenQuorumVerifier() != null
            && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        // 遍历票箱
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            // 若当前遍历的选票与当前要判断的选票vote推荐信息相同，
            // 则将这个选票来源serverId记录到voteSet。即写入到了
            // 跟踪器所包含的所有QuorumVerifier对应的ackset集合中。
            // 也就是说，每个QuorumVerifier对应的ackset集合中的元素，
            // 都是由具有相同推荐信息的serverId构成的，都是“志同道合”的兄弟
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */
        // 将所有leader状态有问题的情况排队掉，那么剩下的就是没有问题的了
        if (leader != self.getId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        LOG.debug(
            "Updating proposal: {} (newleader), 0x{} (newzxid), {} (oldleader), 0x{} (oldzxid)",
            leader,
            Long.toHexString(zxid),
            proposedLeader,
            Long.toHexString(proposedZxid));
        // 更新当前server对于Leader的推荐信息
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    public synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I am a participant: {}", self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I am an observer: {}", self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) {
            return self.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {
        // 若推荐的leader就是当前server自己，则状态ss为LEADING，否则为FOLLOWING
        ServerState ss = (proposedLeader == self.getId()) ? ServerState.LEADING : learningState();
        // 修改当前server的状态
        self.setPeerState(ss);
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    /**
     * 翻译：开启新一轮的Leader选举。无论何时，只要我们的QuorumPeer的
     * 状态变为了LOOKING，那么这个方法将被调用，并且它会发送notifications
     * 给所有其它的同级服务器。
     *
     * 这个方法就是选举的核心方法!!!!!!!!后面专门讲解这个方法
     */
    public Vote lookForLeader() throws InterruptedException {
        // ------------ 1 创建选举对象，做选举前的初始化工作 ---------------------
        try {
            // self为当前参与选举的server对象自己
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        // 记录FLE算法的开始时间
        self.start_fle = Time.currentElapsedTime();
        try {
            /*
             * The votes from the current leader election are stored in recvset. In other words, a vote v is in recvset
             * if v.electionEpoch == logicalclock. The current participant uses recvset to deduce on whether a majority
             * of participants has voted for it.
             */
            // 本集合key为leaderId，value为对应id的投票信息，集合将会记录
            // 本次投票的各个机器投票情况
            // recvset，receive set，用于存放来自于外部的选票，一个entry代表一次投票
            // key为投票者的serverid，value为选票
            // 该集合相当于投票箱
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            /*
             * The votes from previous leader elections, as well as the votes from the current leader election are
             * stored in outofelection. Note that notifications in a LOOKING state are not stored in outofelection.
             * Only FOLLOWING or LEADING notifications are stored in outofelection. The current participant could use
             * outofelection to learn which participant is the leader if it arrives late (i.e., higher logicalclock than
             * the electionEpoch of the received notifications) in a leader election.
             */
            // outofelection，out of election，退出选举
            // 其中存放的是非法选票，即投票者的状态不是looking
            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            // 每次轮询其它机器发来消息的间隔时间，固定200毫秒执行一次
            int notTimeout = minNotificationInterval;

            // ------------ 2 将自己作为初始Leader通知给大家 ---------------------
            synchronized (this) {
                // 逻辑选举次数+1，代表本机器有一次执行了重新选举Leader的操作
                logicalclock.incrementAndGet();
                // 投票前先把本机器的投票信息投给自己，getInitId()为本机器的
                // myid值，getInitLastLoggedZxid()为本机器的zxid值
                // getPeerEpoch()为本机器的currentEpoch值
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info(
                "New election. My id = {}, proposed zxid=0x{}",
                self.getId(),
                Long.toHexString(proposedZxid));
            // 对集群内的各个机器发送消息通知，告诉他们我选举自己当选Leader
            // 此时各个机器的通信对已经创建完毕，因此可以将消息发送给集群内的
            // 各个机器，结果为A->B、A->C通知A当选Leader，B->A，B->C通知B当选
            // Leader，C->B、C->A通知C当选Leader，例子中的三台机器每台机器都
            // 会向集群内其它两台机器发送当选本机器为Leqader的消息通知，当然
            // 也会通知自己，但是通知自己不会经过网络通信
            sendNotifications();

            SyncedLearnerTracker voteSet = null;


            // ------------ 3 验证自己的选票与大家的选票谁更适合做Leader ------
            /*
             * Loop in which we exchange notifications until we find a leader
             */
            // 发完通知消息后开始轮询其它机器的消息
            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                // 轮询集合内是否有其它机器发来的消息，在本次三台机器的集群中，
                // recvqueue.poll()方法一定可以轮询出三个响应消息，其中一个
                // 消息通知为本系统在前面的sendNotifications()方法发出的，没
                // 经过网络通信，而是直接放在了本机的集合中等待处理
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                // poll执行时间超过了200毫秒，阻塞轮询返回结果为空，
                if (n == null) {
                    // 引发外来通知为null的情况有两种，不同情况有不同的处理方案：

                    // manager.haveDelivered()
                    // 为true，说明当前server与集群没有失联
                    // 为false，说明当前server与集群失联
                    if (manager.haveDelivered()) {
                        // 将自己的推荐情况再次向所有其它server发送，以期待其它server
                        // 再次向当前server发送它们的通知
                        sendNotifications();
                    } else {
                        // 与集群中的其它server进行重新连接
                        // 问题：重连后，为什么没有再次调用sendNotifications()向其它Server发送自己的推荐情况？
                        // 两个原因：
                        // 1）队列中的元素会重新发送
                        // 2）只要我失联了，那么其它server就一定不会收齐外来的通知（缺少我的），若在没有收齐的情况
                        // 下还无法选举出新的leader，那么其它server就会出现manager.haveDelivered()为true的情况，
                        // 那么，其它server就会向我发送通知。我只坐等接收即可。
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    // 使下次轮询的等待时间翻倍,轮询等待时间再怎么翻倍也不能超过60000，最多为60s
                    notTimeout = Math.min(notTimeout << 1, maxNotificationInterval);

                    /*
                     * When a leader failure happens on a master, the backup will be supposed to receive the honour from
                     * Oracle and become a leader, but the honour is likely to be delay. We do a re-check once timeout happens
                     *
                     * The leader election algorithm does not provide the ability of electing a leader from a single instance
                     * which is in a configuration of 2 instances.
                     * */
                    if (self.getQuorumVerifier() instanceof QuorumOracleMaj
                            && self.getQuorumVerifier().revalidateVoteset(voteSet, notTimeout != minNotificationInterval)) {
                        setPeerState(proposedLeader, voteSet);
                        Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                        leaveInstance(endVote);
                        return endVote;
                    }

                    LOG.info("Notification time out: {} ms", notTimeout);

                    // 验证选票(验证选举人与被选举人的身份)
                } else if (validVoter(n.sid) && validVoter(n.leader)) {
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    // 判断收到消息的状态，如LOOKING、Leading、Following等
                    switch (n.state) {
                    case LOOKING:
                        if (getInitLastLoggedZxid() == -1) {
                            LOG.debug("Ignoring notification as our zxid is -1");
                            break;
                        }
                        if (n.zxid == -1) {
                            LOG.debug("Ignoring notification from member with -1 zxid {}", n.sid);
                            break;
                        }
                        // If notification > current, replace and send messages out
                        // 收到的消息状态为选举中
                        // 如果发送过来的消息选举迭代次数大于本机的选举迭代次数
                        // 则说明开始需要同步迭代次数并回到正常选举流程
                        if (n.electionEpoch > logicalclock.get()) {
                            // 将消息的选举迭代赋值给本机器的，保证集群内的各个
                            // 机器选举迭代次数一致
                            logicalclock.set(n.electionEpoch);
                            // 清空本机器接收到的各个机器投票情况集合
                            recvset.clear();
                            // 判断发送投票消息的机器与本机器的投票信息
                            // 判断规则逻辑在前面已经说过了假设消息是机器B
                            // 发给机器C的，可以判断出C胜出，因为C的myid大
                            // 因此发送过来的消息对象判断失败
                            // totalOrderPredicate()具体判断规则在最后会
                            // 贴出来
                            if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                // 如果发送过来的消息机器投票信息判断成功
                                // 则把本机的投票信息改成消息的投票信息
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            } else {
                                // 如果本机器胜出，则把投票信息改成本机的
                                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                            }
                            // 向集群内的各个机器发送本机器的投票信息
                            sendNotifications();
                        } else if (n.electionEpoch < logicalclock.get()) {
                                LOG.debug(
                                    "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x{}, logicalclock=0x{}",
                                    Long.toHexString(n.electionEpoch),
                                    Long.toHexString(logicalclock.get()));
                            // 如果是通过WorkerReceiver对象发送进来的这种
                            // 情况在WorkerReceiver接收到时就已经处理过了
                            break;
                        } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                            // 最后一种情况就是n.electionEpoch==logicalclock
                            // 此时说明集群内正在进行正常的选举，一切以正常的
                            // 规则来判断，以经常举例的A、B、C三台机器来说，
                            // 如果是第一次投票，一定会选举机器C来当选Leader
                            // 因此如果是A、B、C这种情况本机器将会把Leader信息
                            // 改成机器C的信息
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            // 向集群内的其它机器发送本机器投票信息
                            sendNotifications();
                        }

                        LOG.debug(
                            "Adding vote: from={}, proposed leader={}, proposed zxid=0x{}, proposed election epoch=0x{}",
                            n.sid,
                            n.leader,
                            Long.toHexString(n.zxid),
                            Long.toHexString(n.electionEpoch));

                        // don't care about the version if it's in LOOKING state
                        // 在本机器记录sid对应的机器投票情况，比如最终A和B肯定
                        // 会把票投给机器C，因此到最后该集合的存储情况会如下：
                        // 机器A：key：sid=1，value：值为机器C的投票信息
                        // 机器B：key：sid=3，value：值为机器C的投票信息
                        // 机器C：key：sid=5，value：值为本机器的投票信息
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                        // ------------ 4 判断本轮选举是否应该结束 ---------------------
                        // 票箱每变化一次，就会创建一次这个选票跟踪器
                        // 注意其两个实参：一个是当前的票箱，一个是当前的server推荐信息构成的选票
                        voteSet = getVoteTracker(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch));

                        // hasAllQuorums方法的作用便是判断recvset集合中是
                        // 有一半以上的值为实例化的Vote对象信息，简单来说就是
                        // 在本机器判断集群内的投票信息是否已有某台机器得票率
                        // 过半了，如果Vote对象的投票过半则说明Leader已经
                        // 选举了出来
                        // 判断当前场景下leader选举是否可以结束了
                        // 当前场景：1）当前票箱 2）当前动态配置
                        if (voteSet.hasAllQuorums()) {

                            // Verify if there is any change in the proposed leader
                            // 轮询recvqueue集合是否还有新的消息可以接收
                            // 为什么这里还需要设置一个循环来轮询recvqueue集
                            // 合呢？这是因为只要执行到这里那么前面一定会更新
                            // 本机器的Leader信息并且向集群的其它机器发送本
                            // 机器的投票信息，此时恰好本机器已经投出了
                            // Leader信息，因此这里需要等待刚刚发出去的消息
                            // 回应收到了
                            while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                // 在确认集群内的其它机器消息时发现有一台机器
                                // 比现在的Leader机器更适合当领导则会退出
                                // 本次循环并且将该通知放入到recvqueue集合
                                // 中以便下次轮询可以查找出来
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                    // 放入recvqueue集合并退出循环
                                    recvqueue.put(n);
                                    break;
                                }
                            }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             */
                            // 如果前面有更适合的Leader则n对象一定不为空，这
                            // 个if判断将不会进去；而如果n为空则说明各个机器
                            // 的回应是没有更适合的Leader信息的，在本机器投票
                            // 成功出来的Leader信息完全可以胜任当选的
                            if (n == null) {
                                // 确认Leader后设置本机器的状态，如果投票的
                                // 机器sid和本机器相等说明本机器就是Leader，
                                // 如果不是则设置成Follower（learningState
                                // 是支持设置成Observer的，单在这里不可能）
                                setPeerState(proposedLeader, voteSet);
                                // 将Leader信息封装成Vote对象
                                Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                // 进入方法打印Leader信息并清空recvqueue集合
                                leaveInstance(endVote);
                                // 返回最终的投票信息，执行到这里说明本机器
                                // 参与的选举流程已经结束了，本机器要么作为
                                // Follower要么作为Leader
                                return endVote;
                            }
                        }
                        break;

                    // ------------ 5 无需选举的情况 ---------------------
                    // 若当前通知是由Observer发来的，则直接结束当前switch-case，
                    // 然后再获取下一个通知。不过，正常情况下，server是不会收到
                    // observer的通知的。这里的代码仅是为了安全考虑的
                    case OBSERVING:
                        LOG.debug("Notification from observer: {}", n.sid);
                        // 如果是观察者则不用做任何事
                        break;

                        /*
                        * In ZOOKEEPER-3922, we separate the behaviors of FOLLOWING and LEADING.
                        * To avoid the duplication of codes, we create a method called followingBehavior which was used to
                        * shared by FOLLOWING and LEADING. This method returns a Vote. When the returned Vote is null, it follows
                        * the original idea to break swtich statement; otherwise, a valid returned Vote indicates, a leader
                        * is generated.
                        *
                        * The reason why we need to separate these behaviors is to make the algorithm runnable for 2-node
                        * setting. An extra condition for generating leader is needed. Due to the majority rule, only when
                        * there is a majority in the voteset, a leader will be generated. However, in a configuration of 2 nodes,
                        * the number to achieve the majority remains 2, which means a recovered node cannot generate a leader which is
                        * the existed leader. Therefore, we need the Oracle to kick in this situation. In a two-node configuration, the Oracle
                        * only grants the permission to maintain the progress to one node. The oracle either grants the permission to the
                        * remained node and makes it a new leader when there is a faulty machine, which is the case to maintain the progress.
                        * Otherwise, the oracle does not grant the permission to the remained node, which further causes a service down.
                        *
                        * In the former case, when a failed server recovers and participate in the leader election, it would not locate a
                        * new leader because there does not exist a majority in the voteset. It fails on the containAllQuorum() infinitely due to
                        * two facts. First one is the fact that it does do not have a majority in the voteset. The other fact is the fact that
                        * the oracle would not give the permission since the oracle already gave the permission to the existed leader, the healthy machine.
                        * Logically, when the oracle replies with negative, it implies the existed leader which is LEADING notification comes from is a valid leader.
                        * To threat this negative replies as a permission to generate the leader is the purpose to separate these two behaviors.
                        *
                        *
                        * */
                    case FOLLOWING:
                        /*
                        * To avoid duplicate codes
                        * */
                        // todo
                        Vote resultFN = receivedFollowingNotification(recvset, outofelection, voteSet, n);
                        if (resultFN == null) {
                            break;
                        } else {
                            return resultFN;
                        }
                    case LEADING:
                        /*
                        * In leadingBehavior(), it performs followingBehvior() first. When followingBehavior() returns
                        * a null pointer, ask Oracle whether to follow this leader.
                        * */
                        // 如果接收到的消息是由Leader或者Follower发过来的
                        // 在同一个选举迭代数中说明是一起选举的，并且本机器
                        // 由于通信晚的原因未能在集群刚好过半的机器中，即未能
                        // 成为第一批修改机器状态的机器，后来收到已经改变状态
                        // 机器的消息时便会进入到这里
                        Vote resultLN = receivedLeadingNotification(recvset, outofelection, voteSet, n);
                        if (resultLN == null) {
                            break;
                        } else {
                            return resultLN;
                        }
                    default:
                        LOG.warn("Notification state unrecognized: {} (n.state), {}(n.sid)", n.state, n.sid);
                        break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    private Vote receivedFollowingNotification(Map<Long, Vote> recvset, Map<Long, Vote> outofelection, SyncedLearnerTracker voteSet, Notification n) {
        /*
         * Consider all notifications from the same epoch
         * together.
         */
        // 若外来通知与当前选举是同一轮
        if (n.electionEpoch == logicalclock.get()) {
            // 进入到这里的消息通知对象包含的Leader信息一定是
            // 真正的Leader信息，因为集群已经选举出了Leader
            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
            voteSet = getVoteTracker(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
            // 该方法会判断recvset的投票数有一台机器是否已经
            // 超过了半数并且判断是否已经真正产生了Leader
            // 很显然一定是的，为什么呢？举个例子，还是A、B、C
            // 三台机器，C被选举成为Leader，而假如A幸运的刚好
            // 变成第二台判断过半数，此时B将会收到A和C的修改
            // 投票通知，收到后B也修改完投票信息，然后再通知
            // A和C，此时A和C将不再是LOOKING状态，便直接由
            // 前面分析过的WorkerReceiver对象响应A和C的投票
            // 信息和状态，此时便会执行到这里
            if (voteSet.hasAllQuorums() && checkLeader(recvset, n.leader, n.electionEpoch)) {
                // 判断成功，根据本机器的id设置状态信息
                setPeerState(n.leader, voteSet);
                // 使用消息通知中的Leader信息封装投票对象
                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                // 进入方法打印Leader信息并清空recvqueue集合
                leaveInstance(endVote);
                // 返回最终的投票信息，执行到这里说明本机器
                // 参与的选举流程已经结束了，本机器要么作为
                // Follower要么作为Leader
                return endVote;
            }
        }

        /*
         * Before joining an established ensemble, verify that
         * a majority are following the same leader.
         *
         * Note that the outofelection map also stores votes from the current leader election.
         * See ZOOKEEPER-1732 for more information.
         */
        // 如果进入到这里说明本机器的选举迭代数和已产生Leader
        // 的集群选举迭代数不是一致的，本机器需要跟随集群走
        // 保存收到的消息对应的机器投票信息
        // 将外来的通知选票放入到“不选举集合”
        outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
        // 创建一个跟踪器，用于判断在outofelection集合中对外来通知中推荐的leader的支持率是否过半
        voteSet = getVoteTracker(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

        // 在加入集群的通信中前需要校验集群的Leader信息
        // 当然outofelection集合中最后肯定是会有超过半数投给
        // 同一个机器且集群的Leader信息会和那个超过半数的机器
        // 信息一致
        if (voteSet.hasAllQuorums() && checkLeader(outofelection, n.leader, n.electionEpoch)) {

            synchronized (this) {
                // 将集群的选举迭代数赋值给本机器以方便集群
                // 机器的信息统一和后续的重新选举
                logicalclock.set(n.electionEpoch);
                // 判断成功，根据本机器的id设置状态信息
                setPeerState(n.leader, voteSet);
            }
            // 使用消息通知中的Leader信息封装投票对象
            Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
            // 进入方法打印Leader信息并清空recvqueue集合
            leaveInstance(endVote);
            // 返回最终的投票信息，执行到这里说明本机器
            // 参与的选举流程已经结束了，本机器要么作为
            // Follower要么作为Leader
            return endVote;
        }

        return null;
    }

    private Vote receivedLeadingNotification(Map<Long, Vote> recvset, Map<Long, Vote> outofelection, SyncedLearnerTracker voteSet, Notification n) {
        /*
        *
        * In a two-node configuration, a recovery nodes cannot locate a leader because of the lack of the majority in the voteset.
        * Therefore, it is the time for Oracle to take place as a tight breaker.
        *
        * */
        Vote result = receivedFollowingNotification(recvset, outofelection, voteSet, n);
        if (result == null) {
            /*
            * Ask Oracle to see if it is okay to follow this leader.
            *
            * We don't need the CheckLeader() because itself cannot be a leader candidate
            * */
            if (self.getQuorumVerifier().getNeedOracle() && !self.getQuorumVerifier().askOracle()) {
                LOG.info("Oracle indicates to follow");
                setPeerState(n.leader, voteSet);
                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                leaveInstance(endVote);
                return endVote;
            } else {
                LOG.info("Oracle indicates not to follow");
                return null;
            }
        } else {
            return result;
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }

}
