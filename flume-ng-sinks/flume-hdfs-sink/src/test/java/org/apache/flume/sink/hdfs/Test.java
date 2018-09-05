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
package org.apache.flume.sink.hdfs;

import java.util.concurrent.*;

public class Test {
    static int idleTimeout = 3;
    static Future idleFuture = null;
    static ScheduledExecutorService timedRollerPool = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            append();
            TimeUnit.SECONDS.sleep(1L);
        }
    }

    public static void append() {
        System.out.println("append");
        if (idleTimeout > 0) {
            // if the future exists and couldn't be cancelled, that would mean it has already run
            // or been cancelled
            boolean a= false;
            if (idleFuture!=null){
                 a=idleFuture.cancel(false);

            }
            if (idleFuture == null || a) {
                Callable<Void> idleAction = new Callable<Void>() {
                    public Void call() throws Exception {
                        System.out.println("Closing ");
                        close();
                        return null;
                    }
                };
                idleFuture = timedRollerPool.schedule(idleAction, idleTimeout,
                        TimeUnit.SECONDS);
            }
        }
    }

    public static void close() {
        System.out.println("Close.");
        idleFuture = null;

    }
}
