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

import java.util.ArrayList;
import java.util.HashSet;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

public class SyncedLearnerTracker {

    protected ArrayList<QuorumVerifierAcksetPair> qvAcksetPairs = new ArrayList<QuorumVerifierAcksetPair>();

    public void addQuorumVerifier(QuorumVerifier qv) {
        // 将指定的验证器构建为一个pair后添加到列表
        qvAcksetPairs.add(new QuorumVerifierAcksetPair(qv, new HashSet<Long>(qv.getVotingMembers().size())));
    }
    // 将sid添加到每个QuorumVerifier对应的ackset集合
    public boolean addAck(Long sid) {
        boolean change = false;
        // 遍历每个QuorumVerifier
        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            // 若当前QuorumVerifier包含的participant中包含当前server，
            // 则将这个serverId写入到这个QuorumVerifier对应的ackset
            if (qvAckset.getQuorumVerifier().getVotingMembers().containsKey(sid)) {
                qvAckset.getAckset().add(sid);
                change = true;
            }
        }
        return change;
    }
    // 判断所有QuorumVerifier对应的ackset中是否包含指定的sid
    public boolean hasSid(long sid) {
        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            if (!qvAckset.getQuorumVerifier().getVotingMembers().containsKey(sid)) {
                // 只要有一个不包含就返回false
                return false;
            }
        }
        // 只要所有的ackset都包含该sid才会返回true
        return true;
    }

    // 验证当前Leader选举是否可以结束了
    public boolean hasAllQuorums() {
        // 遍历所有的QuorumVerifier，只有当所有QuorumVerifier
        // 中的ackset都判断过半了，才能结束本轮leader选举。
        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            if (!qvAckset.getQuorumVerifier().containsQuorum(qvAckset.getAckset())) {
                // 只要有一个QuorumVerifier中的ackset没有过半，就不能结束选举
                return false;
            }
        }
        // 选举可以结束了
        return true;
    }

    public String ackSetsToString() {
        StringBuilder sb = new StringBuilder();

        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            sb.append(qvAckset.getAckset().toString()).append(",");
        }

        return sb.substring(0, sb.length() - 1);
    }

    // 可以将其简单理解为一个key-value对
    public static class QuorumVerifierAcksetPair {
        // 基于ackset验证支持率是否过半
        private final QuorumVerifier qv;
        // 存放的是当前server接收到的外来通知的来源serverId
        private final HashSet<Long> ackset;

        public QuorumVerifierAcksetPair(QuorumVerifier qv, HashSet<Long> ackset) {
            this.qv = qv;
            this.ackset = ackset;
        }

        public QuorumVerifier getQuorumVerifier() {
            return this.qv;
        }

        public HashSet<Long> getAckset() {
            return this.ackset;
        }

    }

}
