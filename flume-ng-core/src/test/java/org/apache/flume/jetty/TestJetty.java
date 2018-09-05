/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.jetty;

import org.apache.flume.instrumentation.http.HTTPMetricsServer;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestJetty extends HTTPMetricsServer {
    @Test
    public void test() throws InterruptedException {
        this.testRandomPortStart();
    }
    @Test
    public void testHost(){
        System.out.println(InetAddressCache.hostName);
        System.out.println(InetAddressCache.hostAddress);
        System.out.println(InetAddressCache.canonicalHostName);
    }
    @Test
    public void testPid(){
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(pid);
    }
    private static final class InetAddressCache {
        static String hostName = null;
        static String hostAddress = null;
        static String canonicalHostName = null;

        static {//todo 规则
            try {
                InetAddress addr = InetAddress.getLocalHost();
                hostName = addr.getHostName();
                hostAddress = addr.getHostAddress();
                canonicalHostName = addr.getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Unable to get localhost", e);
            }
        }
    }
}
