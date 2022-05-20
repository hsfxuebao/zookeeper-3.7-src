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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCnxnSocketNIO extends ClientCnxnSocket {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocketNIO.class);

    // NIO的多路复用选择器
    private final Selector selector = Selector.open();
    // 本Socket对应的SelectionKey
    private SelectionKey sockKey;

    private SocketAddress localSocketAddress;

    private SocketAddress remoteSocketAddress;

    ClientCnxnSocketNIO(ZKClientConfig clientConfig) throws IOException {
        this.clientConfig = clientConfig;
        initProperties();
    }

    @Override
    boolean isConnected() {
        return sockKey != null;
    }

    /**
     * @throws InterruptedException
     * @throws IOException
     */
    void doIO(Queue<Packet> pendingQueue, ClientCnxn cnxn) throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        // 这里有处理OP_READ类型的判断，即处理ZK的Server端传过来的请求
        // 在第一步中不会走到这里面去，因此忽略
        if (sockKey.isReadable()) {
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new EndOfStreamException("Unable to read additional data from server sessionid 0x"
                                               + Long.toHexString(sessionId)
                                               + ", likely server has closed socket");
            }
            if (!incomingBuffer.hasRemaining()) {
                incomingBuffer.flip();
                if (incomingBuffer == lenBuffer) {
                    recvCount.getAndIncrement();
                    readLength();
                } else if (!initialized) {
                    readConnectResult();
                    enableRead();
                    if (findSendablePacket(outgoingQueue, sendThread.tunnelAuthInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite();
                    }
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;
                } else {
                    sendThread.readResponse(incomingBuffer);
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }
        // 处理OP_WRITE类型事件，即处理要发送到ZK的Server端请求包数据
        if (sockKey.isWritable()) {
            // 获取最新的需要发送的数据包，这里获取的便是前面SendThread
            // 放进去的只有ConnectRequest的Packet包对象
            Packet p = findSendablePacket(outgoingQueue, sendThread.tunnelAuthInProgress());

            if (p != null) {
                // 更新最后的发送时间
                updateLastSend();
                // If we already started writing p, p.bb will already exist
                // 如果Packet包的ByteBuffer为空则调用createBB()创建
                // 连接时ByteBuffer是一定为空的，因此这里会一定进入
                if (p.bb == null) {
                    if ((p.requestHeader != null)
                        && (p.requestHeader.getType() != OpCode.ping)
                        && (p.requestHeader.getType() != OpCode.auth)) {
                        p.requestHeader.setXid(cnxn.getXid());
                    }
                    // createBB方法的作用便是序列化请求并将byte[]数组
                    // 添加到ByteBuffer中
                    p.createBB();
                }
                // 使用获取的SocketChannel写入含有序列化数据的ByteBuffer
                sock.write(p.bb);
                if (!p.bb.hasRemaining()) {
                    // 发送成功并删除第一个Packet包对象
                    sentCount.getAndIncrement();
                    outgoingQueue.removeFirstOccurrence(p);
                    // 如果requestHeader不为空，不是ping或者auth类型的
                    // 则将Packet包对象添加到pendingQueue中，代表这个
                    // 包对象正在被Server端处理且没有响应回来
                    // （需要注意的是只有连接时的ConnectRequest请求头
                    // requestHeader才会为空，因此这里的条件便是除了
                    // 新建连接、ping和auth类型的，其它都会被添加进来）
                    if (p.requestHeader != null
                        && p.requestHeader.getType() != OpCode.ping
                        && p.requestHeader.getType() != OpCode.auth) {
                        synchronized (pendingQueue) {
                            pendingQueue.add(p);
                        }
                    }
                }
            }
            // 如果outgoingQueue为空或者尚未连接成功且本次的Packet包对象
            // 已经发送完毕则关闭OP_WRITE操作，因此发送ConnectReuqest请
            // 求后便需要等待Server端的相应确认建立连接，不允许Client端
            // 这边主动发送NIO信息
            if (outgoingQueue.isEmpty()) {
                // No more packets to send: turn off write interest flag.
                // Will be turned on later by a later call to enableWrite(),
                // from within ZooKeeperSaslClient (if client is configured
                // to attempt SASL authentication), or in either doIO() or
                // in doTransport() if not.
                disableWrite();
            } else if (!initialized && p != null && !p.bb.hasRemaining()) {
                // On initial connection, write the complete connect request
                // packet, but then disable further writes until after
                // receiving a successful connection response.  If the
                // session is expired, then the server sends the expiration
                // response and immediately closes its end of the socket.  If
                // the client is simultaneously writing on its end, then the
                // TCP stack may choose to abort with RST, in which case the
                // client would never receive the session expired event.  See
                // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                disableWrite();
            } else {
                // Just in case
                // 为了以防万一打开OP_WRITE操作
                enableWrite();
            }
        }
    }

    private Packet findSendablePacket(LinkedBlockingDeque<Packet> outgoingQueue, boolean tunneledAuthInProgres) {
        // 判断outgoingQueue是否为空
        if (outgoingQueue.isEmpty()) {
            return null;
        }
        // If we've already starting sending the first packet, we better finish
        // 两种条件：
        // 如果第一个的ByteBuffer不为空
        // 如果传入进来的clientTunneledAuthenticationInProgress为false
        // 参数为false说明认证尚未配置或者尚未完成
        if (outgoingQueue.getFirst().bb != null || !tunneledAuthInProgres) {
            return outgoingQueue.getFirst();
        }
        // Since client's authentication with server is in progress,
        // send only the null-header packet queued by primeConnection().
        // This packet must be sent so that the SASL authentication process
        // can proceed, but all other packets should wait until
        // SASL authentication completes.
        // 跑到这里说明认证已完成，需要遍历outgoingQueue数组，把连接的
        // 请求找到并放到队列的第一个，以保证下次读取会读取到连接请求
        Iterator<Packet> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            Packet p = iter.next();
            // 只有连接的requestHeader是空的，因此只需要判断这个条件即可
            // 其它类型的包数据header肯定是不为空的
            if (p.requestHeader == null) {
                // We've found the priming-packet. Move it to the beginning of the queue.
                // 先删除本包，随后放到第一位
                iter.remove();
                outgoingQueue.addFirst(p);
                return p;
            } else {
                // Non-priming packet: defer it until later, leaving it in the queue
                // until authentication completes.
                LOG.debug("Deferring non-priming packet {} until SASL authentication completes.", p);
            }
        }
        // 执行到这里说明确实没有包需要发送
        return null;
    }

    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                LOG.debug("Ignoring exception during shutdown input", e);
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                LOG.debug("Ignoring exception during shutdown output", e);
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                LOG.debug("Ignoring exception during socket close", e);
            }
            try {
                sock.close();
            } catch (IOException e) {
                LOG.debug("Ignoring exception during channel close", e);
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            LOG.debug("SendThread interrupted during sleep, ignoring");
        }
        sockKey = null;
    }

    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }

            selector.close();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }

    /**
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        // 创建一个SocketChannel对象，并设置非阻塞以及其它属性
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     * @param sock the {@link SocketChannel}
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) throws IOException {
        // 将channel注册到selector，并指定其关注事件为客户端连接发起成功
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        // 连接指定地址
        boolean immediateConnect = sock.connect(addr);
        if (immediateConnect) {
            // 这个方法前面已经介绍过了
            sendThread.primeConnection();
        }
    }

    @Override
    void connect(InetSocketAddress addr) throws IOException {
        // 创建一个NIO的channel
        SocketChannel sock = createSock();
        try {
            // 将channel注册到selector
            registerAndConnect(sock, addr);
        } catch (UnresolvedAddressException | UnsupportedAddressTypeException | SecurityException | IOException e) {
            LOG.error("Unable to open socket to {}", addr);
            sock.close();
            throw e;
        }
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     *
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        return remoteSocketAddress;
    }

    /**
     * Returns the local address to which the socket is bound.
     *
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        return localSocketAddress;
    }

    private void updateSocketAddresses() {
        Socket socket = ((SocketChannel) sockKey.channel()).socket();
        localSocketAddress = socket.getLocalSocketAddress();
        remoteSocketAddress = socket.getRemoteSocketAddress();
    }

    @Override
    void packetAdded() {
        wakeupCnxn();
    }

    @Override
    void onClosing() {
        wakeupCnxn();
    }

    private synchronized void wakeupCnxn() {
        selector.wakeup();
    }

    @Override
    void doTransport(
        int waitTimeOut,
        Queue<Packet> pendingQueue,
        ClientCnxn cnxn) throws IOException, InterruptedException {
        // 最多休眠waitTimeOut时间获取NIO事件，调用wake()方法、有可读IO事件和
        // 有OP_WRITE写事件可触发
        selector.select(waitTimeOut);
        Set<SelectionKey> selected;
        synchronized (this) {
            // 获取IO事件保定的SelectionKey对象
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        // 更新now属性为当前时间戳
        updateNow();
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            // 先判断SelectionKey事件是否是连接事件
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                // 如果是连接事件，则调用finishConnect()确保已连接成功
                if (sc.finishConnect()) {
                    // 连接成功后更新发送时间
                    updateLastSendAndHeard();
                    updateSocketAddresses();
                    // 执行主要的接方法，准备发送ZK的连接请连求
                    sendThread.primeConnection();
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                // 再判断是否是OP_READ或者OP_WRITE事件
                // 如果满足则调用doIO方法来处理对应的事件，doIO便是处理获取的
                // IO事件核心方法
                doIO(pendingQueue, cnxn);
            }
        }
        // 执行到这里说明本次触发的NIO事件已经全部执行完毕，但是有可能在途中会
        // 产生新的NIO事件需要执行，因此这里会判断是否有可发送的Packet包，如果有
        // 则开启OP_WRITE操作，以方便下次直接发送
        if (sendThread.getZkState().isConnected()) {
            // 查看是否有可发送的Packet包数据
            if (findSendablePacket(outgoingQueue, sendThread.tunnelAuthInProgress()) != null) {
                // 打开OP_WRITE操作
                enableWrite();
            }
        }
        // 清除SelectionKey集合
        selected.clear();
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        // sockKey may be concurrently accessed by multiple
        // threads. We use tmp here to avoid a race condition
        SelectionKey tmp = sockKey;
        if (tmp != null) {
            ((SocketChannel) tmp.channel()).socket().close();
        }
    }

    @Override
    void saslCompleted() {
        enableWrite();
    }

    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    private synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    private synchronized void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    void connectionPrimed() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }

}
