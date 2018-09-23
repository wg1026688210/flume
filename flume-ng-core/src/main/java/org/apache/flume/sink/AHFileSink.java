/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.mina.util.CopyOnWriteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Contended;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AHFileSink extends AbstractSink implements Configurable {//todo 将同步和检查过期句柄写在一个线程
    private static final Logger logger = LoggerFactory.getLogger(AHFileSink.class);
    private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");
    private static final String BATCH_SIZE_CONF_NAME = "sink.batchSize";
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final String DIRTECTORY_CONF_NAME = "sink.directory";
    private static final String SERIALIZER_TYPE_CONF_NAME = "sink.serializer";
    private static final String DEFAULT_SERIALIZER_TYPE_CONF_NAME = "TEXT";
    private static final String CHECK_INTERVAL_CONF_NAME = "sink.checkInterval";
    private static final String IDLE_TIME_CONF_NAME = "sink.idleTime";
    private static final int DEFAULT_CHECK_INTERVAL = 10;
    private static final String EVENT_HEADER_FILE_KEY = "file";
    private static final String UNKNOW_FILE = "UNKNOW_FILE";
    private static final String FILE_WITH_HOST_CONF_NAME ="sink.fileWithHost";
    private static final boolean DEFAULT_FILE_WITH_HOST_CONF_NAME=false;
//    private String host ;//域名
    private String serializerType;//写文件的类型
    private int idleTime;//超时时间
    private int batchSize;//批量写的大小
    private File directory;//写入目录
    private int checkInterval;//check句柄的周期
    private boolean fileWithHost;//是否文件带域名~
    private SinkCounter sinkCounter;
    private ScheduledExecutorService checkService;//定时去扫超时的句柄
    private Map<String, FileDes> fileDesCache = new ConcurrentHashMap<>();//文件句柄缓存
    private Map<String, FileDes> filesOutTime = new CopyOnWriteMap<>();//超时的句柄缓存;
    private Context serializerContext;//写文件配置的上下文
    @Contended
    class FileDes {
        volatile long updateTime;//updateTime更新的时间
        String fileName;//文件名
        OutputStream outputStream;//输出流
        EventSerializer serializer;//写event,序列化类

        public FileDes(long updateTime, String fileName, OutputStream outputStream, EventSerializer serializer) {
            this.updateTime = updateTime;
            this.fileName = fileName;
            this.outputStream = outputStream;
            this.serializer = serializer;
        }

        public FileDes() {
        }

        public FileDes modifyUpdateTime(long time) {
            updateTime = time;
            return this;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public EventSerializer getSerializer() {
            return serializer;
        }

        public void setSerializer(EventSerializer serializer) {
            this.serializer = serializer;
        }

        public FileDes self() {
            return this;
        }

        public void setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public OutputStream getOutputStream() {
            return outputStream;
        }

        public void setOutputStream(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        public void write(Event event) throws IOException {
            this.serializer.write(event);
        }

        public void flush() throws IOException {
            this.serializer.flush();
            this.outputStream.flush();
        }

        void closeFile() throws IOException {
            this.serializer.flush();
            this.serializer.beforeClose();
            this.outputStream.flush();
            this.outputStream.close();
            AHFileSink.this.sinkCounter.incrementConnectionClosedCount();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        Status status = Status.READY;
        try {
            transaction.begin();
            int eventAttemptCounter = 0;

            HashSet<FileDes> fileDesUsed = new HashSet<>();//这批数据使用的文件句柄缓存起来
            long nowTime = System.currentTimeMillis();
            for (int i = 0; i < batchSize; ++i) {//开始搂数据
                FileDes fileDes = null;
                event = channel.take();
                if (event != null) {
                    sinkCounter.incrementEventDrainAttemptCount();
                    eventAttemptCounter++;

                    Map<String, String> headers = event.getHeaders();
                    String syncAbsolutePath = headers.get(AHFileSink.EVENT_HEADER_FILE_KEY);//同步的文件
                    String syncFile = syncAbsolutePath != null ? syncAbsolutePath : AHFileSink.UNKNOW_FILE;//获取服务端的文件名
                    if (this.fileDesCache.containsKey(syncFile)) {
                        fileDes = this.fileDesCache.get(syncFile).modifyUpdateTime(nowTime);
                    } else {//没有缓存这个文件的句柄就建一个新的句柄；
                        fileDes = this.newFileDes(syncFile,headers);
                        this.fileDesCache.put(syncFile, fileDes);
                    }
                    fileDes.write(event);
                    fileDesUsed.add(fileDes);
                } else {//搂不到数据就歇一会
                    sinkCounter.incrementBatchEmptyCount();
                    status = Status.BACKOFF;
                    break;
                }
            }
            for (FileDes des : fileDesUsed) {//批量把句柄关掉。。。
                des.flush();
            }
            this.closeOutTimeFileDes();//把超时的文件句柄关掉
            transaction.commit();//加成功了提交事务
            sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
        } catch (IOException e) {//出现异常，事务回滚
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", e);
        } finally {//关闭事务
            transaction.close();
        }

        return status;
    }

    private FileDes newFileDes(String syncFile,Map<String,String> headers) throws FileNotFoundException {

        String fileName = this.getFileNameFromAbsolutePath(syncFile,headers);//文件名
        File absolutePathSync = new File(this.directory, fileName);//终极文件目录,啊哈哈
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(absolutePathSync, true));//建一个可追加的流
        long now = System.currentTimeMillis();//当前时间
        EventSerializer serializer = EventSerializerFactory.getInstance(
                this.serializerType, this.serializerContext, outputStream
        );
        FileDes fileDes = new FileDes(now, fileName, outputStream, serializer);
        return fileDes;
    }

    private String getFileNameFromAbsolutePath(String syncFile,Map<String,String > headers) {
        int index = syncFile.lastIndexOf(DIRECTORY_DELIMITER);
        String fileName = syncFile.substring(index + 1);
        if (this.fileWithHost){
            String host = headers.getOrDefault("host", "UNKNOWHOST");
            fileName =host+"-"+fileName;
        }
        return fileName;
    }

    private void closeOutTimeFileDes() throws IOException {
        Map<String, FileDes> filesOutTime = this.filesOutTime;
        for (Map.Entry<String, FileDes> entry : filesOutTime.entrySet()) {
            FileDes file = entry.getValue();
            String fileName = entry.getKey();
            file.closeFile();
            this.fileDesCache.remove(fileName);
        }
    }

    @Override
    public void configure(Context context) {
        String directory = context.getString(AHFileSink.DIRTECTORY_CONF_NAME);
        Preconditions.checkArgument(directory != null, "Directory may not be null");
        this.batchSize = context.getInteger(AHFileSink.BATCH_SIZE_CONF_NAME, DEFAULT_BATCH_SIZE);
        serializerType = context.getString(AHFileSink.SERIALIZER_TYPE_CONF_NAME, AHFileSink.DEFAULT_SERIALIZER_TYPE_CONF_NAME);
        serializerContext =
                new Context(context.getSubProperties("sink." +
                        EventSerializer.CTX_PREFIX));
        this.directory = new File(directory);
        this.idleTime = context.getInteger(AHFileSink.IDLE_TIME_CONF_NAME, 30000);
        this.checkInterval = context.getInteger(AHFileSink.CHECK_INTERVAL_CONF_NAME, DEFAULT_CHECK_INTERVAL);
        this.fileWithHost =context.getBoolean(AHFileSink.FILE_WITH_HOST_CONF_NAME,DEFAULT_FILE_WITH_HOST_CONF_NAME);
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(this.getName());
        }

    }

    @Override
    public void start() {
        logger.info("Starting {}.....", this);
        checkService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat(
                "rollingFileSink-roller-" +
                        Thread.currentThread().getId() + "-%d").build());
        checkService.scheduleAtFixedRate(new ReleaseFileHandler(), this.checkInterval, this.checkInterval, TimeUnit.SECONDS);

        sinkCounter.start();
        super.start();

    }

    @Override
    public synchronized void stop() {
        logger.info("把这个AHFileSink关掉.....{}", getName());
        super.stop();
        sinkCounter.stop();
        this.fileDesCache.forEach(
                (k, v) -> {
                    try {
                        v.closeFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
        this.fileDesCache.clear();
        checkService.shutdown();
        while (!checkService.isTerminated()) {
            try {
                checkService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for roll service to stop. " +
                        "Please report this.", e);
            }
        }
        logger.info("AHFileSink sink {} stopped. Event metrics: {}",
                getName(), sinkCounter);
    }

    class ReleaseFileHandler implements Runnable {
        @Override
        public void run() {
            int outTime = AHFileSink.this.idleTime;
            long now = System.currentTimeMillis();
            Map<String, FileDes> temMap = AHFileSink.this.fileDesCache
                    .entrySet()
                    .stream()
                    .filter(s -> s.getValue().updateTime + outTime < now)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));//找到超时的句柄
            AHFileSink.this.filesOutTime.putAll(temMap);
        }
    }

}
