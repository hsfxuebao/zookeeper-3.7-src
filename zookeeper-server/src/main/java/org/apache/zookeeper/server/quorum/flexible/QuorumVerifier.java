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

package org.apache.zookeeper.server.quorum.flexible;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LearnerHandler;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.SyncedLearnerTracker;

/**
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server
 * identifiers constitutes a quorum.
 *
 * 所有quorum验证器都必须实现一个名为containsQuorum的方法，
 * 该方法验证ServerId的HashSet是否构成大多数
 */

// QuorumVerifier其实对应的是一个版本的zoo.cfg.dynamic的动态配置
public interface QuorumVerifier {

    // 获取指定server的weight权重
    // 性能越好的server一般要设置越大的权重
    long getWeight(long id);
    // 用于判断给定的set集合中包含的serverId是否已经达到了过半（大多数）
    boolean containsQuorum(Set<Long> set);
    // 其对应的就是zoo.cfg.dynamic的版本
    long getVersion();
    void setVersion(long ver);

    // 获取动态配置文件中不同类型的server集合
    Map<Long, QuorumServer> getAllMembers();
    Map<Long, QuorumServer> getVotingMembers();
    Map<Long, QuorumServer> getObservingMembers();
    boolean equals(Object o);
    /*
    * Only QuorumOracleMaj will implement these methods. Other class will raise warning if the methods are called and
    * return false always.
    * */
    default boolean updateNeedOracle(List<LearnerHandler> forwardingFollowers) {
        return false;
    }
    default boolean getNeedOracle() {
        return false;
    }

    default boolean askOracle() {
        return false;
    }

    default boolean overrideQuorumDecision(List<LearnerHandler> forwardingFollowers) {
        return false;
    }

    default boolean revalidateOutstandingProp(Leader self, ArrayList<Leader.Proposal> outstandingProposal, long lastCommitted) {
        return false;
    }

    default boolean revalidateVoteset(SyncedLearnerTracker voteSet, boolean timeout) {
        return false;
    }

    default String getOraclePath() {
        return null;
    };

    String toString();

}
