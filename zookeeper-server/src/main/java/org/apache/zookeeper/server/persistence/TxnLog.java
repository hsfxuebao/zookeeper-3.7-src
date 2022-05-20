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

package org.apache.zookeeper.server.persistence;

import java.io.Closeable;
import java.io.IOException;
import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Interface for reading transaction logs.
 *
 */
public interface TxnLog extends Closeable {

    /**
     * Setter for ServerStats to monitor fsync threshold exceed
     * @param serverStats used to update fsyncThresholdExceedCount
     */
    // 设置服务状态
    void setServerStats(ServerStats serverStats);

    /**
     * roll the current
     * log being appended to
     * @throws IOException
     */
    // 滚动日志
    void rollLog() throws IOException;
    /**
     * Append a request to the transaction log
     * @param hdr the transaction header
     * @param r the transaction itself
     * @return true iff something appended, otw false
     * @throws IOException
     */
    // 追加
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * Append a request to the transaction log with a digset
     * @param hdr the transaction header
     * @param r the transaction itself
     * @param digest transaction digest
     * returns true iff something appended, otw false
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r, TxnDigest digest) throws IOException;

    /**
     * Start reading the transaction logs
     * from a given zxid
     * @param zxid
     * @return returns an iterator to read the
     * next transaction in the logs.
     * @throws IOException
     */
    // 读取数据
    TxnIterator read(long zxid) throws IOException;

    /**
     * the last zxid of the logged transactions.
     * @return the last zxid of the logged transactions.
     * @throws IOException
     */
    // 获取最后一个zxid
    long getLastLoggedZxid() throws IOException;

    /**
     * truncate the log to get in sync with the
     * leader.
     * @param zxid the zxid to truncate at.
     * @throws IOException
     */
    // 删除日志
    boolean truncate(long zxid) throws IOException;

    /**
     * the dbid for this transaction log.
     * @return the dbid for this transaction log.
     * @throws IOException
     */
    long getDbId() throws IOException;

    /**
     * commit the transaction and make sure
     * they are persisted
     * @throws IOException
     */
    // 提交
    void commit() throws IOException;

    /**
     *
     * @return transaction log's elapsed sync time in milliseconds
     */
    // 日志同步时间
    long getTxnLogSyncElapsedTime();

    /**
     * close the transactions logs
     */
    // 关闭日志
    void close() throws IOException;

    /**
     * Sets the total size of all log files
     */
    void setTotalLogSize(long size);

    /**
     * Gets the total size of all log files
     */
    long getTotalLogSize();

    /**
     * an iterating interface for reading
     * transaction logs.
     */
    // 读取日志的接口
    interface TxnIterator extends Closeable {

        /**
         * return the transaction header.
         * @return return the transaction header.
         */
        // 获取头信息
        TxnHeader getHeader();

        /**
         * return the transaction record.
         * @return return the transaction record.
         */
        // 获取传输的内容
        Record getTxn();

        /**
         * @return the digest associated with the transaction.
         */
        //
        TxnDigest getDigest();

        /**
         * go to the next transaction record.
         * @throws IOException
         */
        // 下一条记录
        boolean next() throws IOException;

        /**
         * close files and release the
         * resources
         * @throws IOException
         */
        // 关闭资源
        void close() throws IOException;

        /**
         * Get an estimated storage space used to store transaction records
         * that will return by this iterator
         * @throws IOException
         */
        // 获取存储的大小
        long getStorageSize() throws IOException;

    }

}

