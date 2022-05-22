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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a validator for hierarchical quorums. With this
 * construction, zookeeper servers are split into disjoint groups, and
 * each server has a weight. We obtain a quorum if we get more than half
 * of the total weight of a group for a majority of groups.
 *
 * 这个类实现了一个层次量化的验证器。通过这种结构，zookeeper服务器被分成不相交的组，并且
 *  * 每个服务器都有一个权重。对于大多数group，如果我们获得的权重超过这个group总权重的一半，
 *  * 我们就获得大多数（过半）。
 *
 * The configuration of quorums uses two parameters: group and weight.
 * Groups are sets of ZooKeeper servers, and we set a group by passing
 * a colon-separated list of server ids. It is also necessary to assign
 * weights to server. Here is an example of a configuration that creates
 * three groups and assigns a weight of 1 to each server:
 *
 * quorums的配置使用两个参数：组和权重。组是ZooKeeper服务器的集合，
 *  * 我们设置一个组，通过传递一个以冒号分隔的server id列表。它还需要为server分配权重。
 *  * 以下是一个配置示例，该配置创建三个group并为每个server分配权重为1：
 *
 *  group.1=1:2:3
 *  group.2=4:5:6
 *  group.3=7:8:9
 *
 *  weight.1=1
 *  weight.2=1
 *  weight.3=1
 *  weight.4=1
 *  weight.5=1
 *  weight.6=1
 *  weight.7=1
 *  weight.8=1
 *  weight.9=1
 *
 * 注意，默认情况下，每个server就是一个group，groupId就是severId；
 *  每个participant的weight默认为1，每个observer的weight默认为0
 * Note that it is still necessary to define peers using the server keyword.
 *
 * 请注意，仍然需要使用server关键字定义对等点。
 *   server.1=...
 *   server.2=...
 *   server.3=...
 */

public class QuorumHierarchical implements QuorumVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumHierarchical.class);

    // key为serverId，value为该server的weight
    private HashMap<Long, Long> serverWeight = new HashMap<Long, Long>();
    // key为serverId，value为该server所属的groupId
    private HashMap<Long, Long> serverGroup = new HashMap<Long, Long>();
    // key为groupId，value为该group所包含的的所有server的权重之和
    private HashMap<Long, Long> groupWeight = new HashMap<Long, Long>();

    // 集群中包含的组的数量。
    // 若每个server为一个group，那么该变量就变为了server的数量
    private int numGroups = 0;

    // 存放当前版本的动态配置中所有的server
    private Map<Long, QuorumServer> allMembers = new HashMap<Long, QuorumServer>();
    // 存放当前版本的动态配置中所有的participant
    private Map<Long, QuorumServer> participatingMembers = new HashMap<Long, QuorumServer>();
    // 存放当前版本的动态配置中所有的observer
    private Map<Long, QuorumServer> observingMembers = new HashMap<Long, QuorumServer>();
    // 当前动态配置的版本号
    private long version = 0;

    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }

    public boolean equals(Object o) {
        if (!(o instanceof QuorumHierarchical)) {
            return false;
        }
        QuorumHierarchical qm = (QuorumHierarchical) o;
        if (qm.getVersion() == version) {
            return true;
        }
        if ((allMembers.size() != qm.getAllMembers().size())
            || (serverWeight.size() != qm.serverWeight.size())
            || (groupWeight.size() != qm.groupWeight.size())
            || (serverGroup.size() != qm.serverGroup.size())) {
            return false;
        }
        for (QuorumServer qs : allMembers.values()) {
            QuorumServer qso = qm.getAllMembers().get(qs.id);
            if (qso == null || !qs.equals(qso)) {
                return false;
            }
        }
        for (Entry<Long, Long> entry : serverWeight.entrySet()) {
            if (!entry.getValue().equals(qm.serverWeight.get(entry.getKey()))) {
                return false;
            }
        }
        for (Entry<Long, Long> entry : groupWeight.entrySet()) {
            if (!entry.getValue().equals(qm.groupWeight.get(entry.getKey()))) {
                return false;
            }
        }
        for (Entry<Long, Long> entry : serverGroup.entrySet()) {
            if (!entry.getValue().equals(qm.serverGroup.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * This constructor requires the quorum configuration
     * to be declared in a separate file, and it takes the
     * file as an input parameter.
     */
    public QuorumHierarchical(String filename) throws ConfigException {
        readConfigFile(filename);
    }

    /**
     * This constructor takes a set of properties. We use
     * it in the unit test for this feature.
     */

    public QuorumHierarchical(Properties qp) throws ConfigException {
        parse(qp);
        LOG.info("{}, {}, {}", serverWeight.size(), serverGroup.size(), groupWeight.size());
    }

    /**
     * Returns the weight of a server.
     *
     * @param id
     */
    public long getWeight(long id) {
        return serverWeight.get(id);
    }

    /**
     * Reads a configuration file. Called from the constructor
     * that takes a file as an input.
     */
    private void readConfigFile(String filename) throws ConfigException {
        File configFile = new File(filename);

        LOG.info("Reading configuration from: {}", configFile);

        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString() + " file is missing");
            }

            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }

            parse(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + filename, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + filename, e);
        }

    }

    /**
     * Parse properties if configuration given in a separate file.
     * Assumes that allMembers has been already assigned
     * @throws ConfigException
     */
    private void parse(Properties quorumProp) throws ConfigException {
        for (Entry<Object, Object> entry : quorumProp.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();

            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                QuorumServer qs = new QuorumServer(sid, value);
                allMembers.put(Long.valueOf(sid), qs);
                if (qs.type == LearnerType.PARTICIPANT) {
                    participatingMembers.put(Long.valueOf(sid), qs);
                } else {
                    observingMembers.put(Long.valueOf(sid), qs);
                }
            } else if (key.startsWith("group")) {
                int dot = key.indexOf('.');
                long gid = Long.parseLong(key.substring(dot + 1));

                numGroups++;

                String[] parts = value.split(":");
                for (String s : parts) {
                    long sid = Long.parseLong(s);
                    if (serverGroup.containsKey(sid)) {
                        throw new ConfigException("Server " + sid + "is in multiple groups");
                    } else {
                        serverGroup.put(sid, gid);
                    }
                }

            } else if (key.startsWith("weight")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                serverWeight.put(sid, Long.parseLong(value));
            } else if (key.equals("version")) {
                version = Long.parseLong(value, 16);
            }
        }

        for (QuorumServer qs : allMembers.values()) {
            Long id = qs.id;
            if (qs.type == LearnerType.PARTICIPANT) {
                if (!serverGroup.containsKey(id)) {
                    throw new ConfigException("Server " + id + "is not in a group");
                }
                if (!serverWeight.containsKey(id)) {
                    serverWeight.put(id, (long) 1);
                }
            }
        }

        computeGroupWeight();
    }

    public Map<Long, QuorumServer> getAllMembers() {
        return allMembers;
    }
    public String toString() {
        StringWriter sw = new StringWriter();

        for (QuorumServer member : getAllMembers().values()) {
            String key = "server." + member.id;
            String value = member.toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');
        }

        Map<Long, String> groups = new HashMap<Long, String>();
        for (Entry<Long, Long> pair : serverGroup.entrySet()) {
            Long sid = pair.getKey();
            Long gid = pair.getValue();
            String str = groups.get(gid);
            if (str == null) {
                str = sid.toString();
            } else {
                str = str.concat(":").concat(sid.toString());
            }
            groups.put(gid, str);
        }

        for (Entry<Long, String> pair : groups.entrySet()) {
            Long gid = pair.getKey();
            String key = "group." + gid.toString();
            String value = pair.getValue();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');
        }

        for (Entry<Long, Long> pair : serverWeight.entrySet()) {
            Long sid = pair.getKey();
            String key = "weight." + sid.toString();
            String value = pair.getValue().toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');
        }

        sw.append("version=" + Long.toHexString(version));

        return sw.toString();
    }

    /**
     * This method pre-computes the weights of groups to speed up processing
     * when validating a given set. We compute the weights of groups in
     * different places, so we have a separate method.
     */
    private void computeGroupWeight() {
        for (Entry<Long, Long> entry : serverGroup.entrySet()) {
            Long sid = entry.getKey();
            Long gid = entry.getValue();
            if (!groupWeight.containsKey(gid)) {
                groupWeight.put(gid, serverWeight.get(sid));
            } else {
                long totalWeight = serverWeight.get(sid) + groupWeight.get(gid);
                groupWeight.put(gid, totalWeight);
            }
        }

        /*
         * Do not consider groups with weight zero
         */
        for (long weight : groupWeight.values()) {
            LOG.debug("Group weight: {}", weight);
            if (weight == ((long) 0)) {
                numGroups--;
                LOG.debug("One zero-weight group: 1, {}", numGroups);
            }
        }
    }

    /**
     * Verifies if a given set is a quorum.
     * 验证给定的set是否是大多数
     */
    public boolean containsQuorum(Set<Long> set) {
        // 临时集合变量
        // key为groupId，value为当前set中该groupId的所有server的weight之和
        HashMap<Long, Long> expansion = new HashMap<Long, Long>();

        /*
         * Adds up weights per group
         */
        LOG.debug("Set size: {}", set.size());
        if (set.size() == 0) {
            return false;
        }

        for (long sid : set) {
            // 获取当前遍历的server所属的groupId
            Long gid = serverGroup.get(sid);
            if (gid == null) {
                continue;
            }
            // 若当前expansion中没有该gid，那么当前遍历的这个server就是这个group的第一个server，
            // 就将当前遍历server的weight作为该group的权重之和写入expansion
            if (!expansion.containsKey(gid)) {
                expansion.put(gid, serverWeight.get(sid));
            } else {
                // 若当前expansion中已经存在这个gid了，那么就将当前遍历server的weight追加到这个
                // group的weight之和中
                long totalWeight = serverWeight.get(sid) + expansion.get(gid);
                expansion.put(gid, totalWeight);
            }
        }

        /*
         * Check if all groups have majority
         */
        int majGroupCounter = 0;
        for (Entry<Long, Long> entry : expansion.entrySet()) {
            // 获取当前遍历的group
            Long gid = entry.getKey();
            LOG.debug("Group info: {}, {}, {}", entry.getValue(), gid, groupWeight.get(gid));
            // entry.getValue() 是当前遍历group的totalWeight
            // 判断当前遍历group的totalWeight是否大于当前组总权重之和的一半，
            // 若大于，计数器加一
            if (entry.getValue() > (groupWeight.get(gid) / 2)) {
                majGroupCounter++;
            }
        }

        LOG.debug("Majority group counter: {}, {}", majGroupCounter, numGroups);
        // 若大多数group都满足前面的条件，则返回true，
        // 表示当前版本的QuorumVerifier对ackset的判断是过半的
        if ((majGroupCounter > (numGroups / 2))) {
            LOG.debug("Positive set size: {}", set.size());
            return true;
        } else {
            LOG.debug("Negative set size: {}", set.size());
            return false;
        }
    }
    public Map<Long, QuorumServer> getVotingMembers() {
        return participatingMembers;
    }

    public Map<Long, QuorumServer> getObservingMembers() {
        return observingMembers;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long ver) {
        version = ver;
    }

}
