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
package org.apache.flume.instrumentation.http;

import com.apache.flume.entity.InstanceDetails;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Monitor service implementation that runs a web server on a configurable
 * port and returns the metrics for components in JSON format. <p> Optional
 * parameters: <p> <tt>port</tt> : The port on which the server should listen
 * to.<p> Returns metrics in the following format: <p>
 * <p>
 * {<p> "componentName1":{"metric1" : "metricValue1","metric2":"metricValue2"}
 * <p> "componentName1":{"metric3" : "metricValue3","metric4":"metricValue4"}
 * <p> }
 */
public class HTTPMetricsServer implements MonitorService {

    private Server jettyServer;
    private int port;
    private static Logger LOG = LoggerFactory.getLogger(HTTPMetricsServer.class);
    public static int DEFAULT_PORT = 0;
    public static String CONFIG_PORT = "port";
    public static String CONFIG_REGISTERABLE = "registerable";
    public static String CONFIG_NAME = "name";
    public static String CONFIG_ZKPATH = "zkPath";
    public static String CONFIG_ZKCONNECTION = "zkCon";
    public static String CONFIG_DESC = "desc";
    public static String CONFIG_LOGPATH ="logPath";
    public int portUsed;
    private static final String basePath = "/base/flume";

    private String name;
    private boolean registerable;
    private String zkPath;
    private String zkCon;
    private String desc;
    private String logPath;
    CuratorFramework client ;

    private String hostName ;
    private String hostAddress ;
    private String canonicalHostName ;

    private ZKServer zkServer ;

    @Override
    public boolean start()  {
        boolean result =true;
        while(true){
            jettyServer = new Server();
            //We can use Contexts etc if we have many urls to handle. For one url,
            //specifying a handler directly is the most efficient.
            SelectChannelConnector connector = new SelectChannelConnector();
            connector.setReuseAddress(true);
            connector.setPort(this.port);
            jettyServer.setConnectors(new Connector[]{connector});
            jettyServer.setHandler(new HTTPMetricsHandler());
            try {
                jettyServer.start();
                while (!jettyServer.isStarted()) {
                    Thread.sleep(500);
                }
                portUsed=Arrays.stream(jettyServer.getConnectors()).map(s->s.getLocalPort()).findAny().orElse(0);
                LOG.warn("你木有设端口，我自动给你添了个"+this.canonicalHostName+":"+portUsed);
                register();
            } catch (Exception e) {
                e.printStackTrace();
                if (e instanceof BindException&&e.getMessage().matches(".*Address already in use.*")){
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    continue;
                }
                result =false;
            }
            break;

        }
        return true;

    }
    @VisibleForTesting
    protected void testRandomPortStart() throws InterruptedException {
        while(true){
            jettyServer = new Server();
            //We can use Contexts etc if we have many urls to handle. For one url,
            //specifying a handler directly is the most efficient.
            SelectChannelConnector connector = new SelectChannelConnector();
            connector.setReuseAddress(true);
            connector.setPort(0);
            jettyServer.setConnectors(new Connector[]{connector});
            jettyServer.setHandler(new HTTPMetricsHandler());
            try {
                jettyServer.start();
                while (!jettyServer.isStarted()) {
                    Thread.sleep(500);
                }
                portUsed=Arrays.stream(jettyServer.getConnectors()).map(s->s.getLocalPort()).findAny().orElse(0);

            } catch (Exception e) {
                e.printStackTrace();
                if (e instanceof BindException&&e.getMessage().matches(".*Address already in use.*")){
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    continue;
                }

            }
            break;

        }


    }


    @Override
    public void stop() {
        try {
            jettyServer.stop();
            jettyServer.join();
            CloseableUtils.closeQuietly(this.zkServer);
            CloseableUtils.closeQuietly(this.client);
            Thread.sleep(5000);
        } catch (Exception ex) {
            LOG.error("Error stopping Jetty. JSON Metrics may not be available.", ex);
        }

    }

    @Override
    public void configure(Context context) {
        this.port = context.getInteger(CONFIG_PORT, DEFAULT_PORT);
        this.registerable = context.getBoolean(CONFIG_REGISTERABLE, false);
        this.name = context.getString(CONFIG_NAME, "undefine");
        this.zkPath = context.getString(CONFIG_ZKPATH, HTTPMetricsServer.basePath);
        this.zkCon = context.getString(HTTPMetricsServer.CONFIG_ZKCONNECTION);
        this.desc =context.getString(HTTPMetricsServer.CONFIG_DESC,"");
        this.logPath =context.getString(HTTPMetricsServer.CONFIG_LOGPATH,"");
        try {
            InetAddress addr = InetAddress.getLocalHost();
            this.hostAddress =addr.getHostAddress();
            this.hostName =addr.getHostName();
            this.canonicalHostName =addr.getCanonicalHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


    public void register() {
        if (!this.registerable) {
            return;
        }

        try {
            client = CuratorFrameworkFactory.newClient(zkCon, new ExponentialBackoffRetry(1000, 3));
            client.start();
            client.blockUntilConnected();
            JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);
            UriSpec uriSpec = new UriSpec("{scheme}://{address}:{port}/metrics");
            String hostName =this.canonicalHostName;
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            String id =pid+"@"+hostName;
            this.desc =this.wrapInstanceDesc(this.desc);
            ServiceInstance<InstanceDetails> serviceInstance = ServiceInstance.
                    <InstanceDetails>builder()
                    .address(hostName)
                    .id(id)
                    .name(this.name)
                    .payload(new InstanceDetails(this.desc,this.logPath,id))
                    .port(this.portUsed)
                    .uriSpec(uriSpec)
                    .build();
            zkServer = new ZKServer(this.client, this.zkPath, serviceInstance);
            zkServer.start();
        } catch (InterruptedException e) {
            LOG.error("连不上哈~",e);
        } catch (Exception e) {
            LOG.error("zk处理异常",e);
        }

    }
    private String wrapInstanceDesc(String desc){

        return new StringBuilder(desc)
                .append("@@@@@@@")
                .append(this.canonicalHostName)
                .append("@")
                .append(this.hostName)
                .append("@")
                .append(this.hostAddress)
                .toString();
    }

    private class HTTPMetricsHandler extends AbstractHandler {

        Type mapType = new TypeToken<Map<String, Map<String, String>>>() {
        }.getType();
        Gson gson = new Gson();

        @Override
        public void handle(String target,
                           HttpServletRequest request,
                           HttpServletResponse response,
                           int dispatch) throws IOException, ServletException {
            // /metrics is the only place to pull metrics.
            //If we want to use any other url for something else, we should make sure
            //that for metrics only /metrics is used to prevent backward
            //compatibility issues.
            if (request.getMethod().equalsIgnoreCase("TRACE") ||
                    request.getMethod().equalsIgnoreCase("OPTIONS")) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                response.flushBuffer();
                ((Request) request).setHandled(true);
                return;
            }
            if (target.equals("/")) {
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write("For Flume metrics please click"
                        + " <a href = \"./metrics\"> here</a>.");
                response.flushBuffer();
                ((Request) request).setHandled(true);
                return;
            } else if (target.equalsIgnoreCase("/metrics")) {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
                String json = gson.toJson(metricsMap, mapType);
                response.getWriter().write(json);
                response.flushBuffer();
                ((Request) request).setHandled(true);
                return;
            }
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            response.flushBuffer();
            //Not handling the request returns a Not found error page.
        }
    }

}
