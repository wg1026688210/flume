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

package org.apache.flume.source.taildir;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

    private final List<TaildirMatcher> taildirCache;
    private final Table<String, String, String> headerTable;
    private static final String HOUR_HEADER = "hour";
    private static final String DATE_HEADER = "date";
    private static final String HOST_HEADER = "host";
    private TailFile currentFile = null;
    private Map<Long, TailFile> tailFiles = Maps.newHashMap();
    private long updateTime;
    private boolean addByteOffset;
    private boolean cachePatternMatching;
    private boolean committed = true;
    private final boolean annotateFileName;
    private final String fileNameHeader;
    private String format;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter dateHeaderFormatter;
    private boolean isFileTimeEnable;//是否能从文件名提取时间
    private boolean isFileHostEnable;//是否能从文件名提取域名

    /**
     * Create a ReliableTaildirEventReader to watch the given directory.
     */
    private ReliableTaildirEventReader(Map<String, String> filePaths,
                                       Table<String, String, String> headerTable, String positionFilePath,
                                       boolean skipToEnd, boolean addByteOffset, boolean cachePatternMatching,
                                       boolean annotateFileName, String fileNameHeader, String dateFileFormat, String dateHeaderFormat,boolean isFileHostEnable) throws IOException {
        // Sanity checks
        Preconditions.checkNotNull(filePaths);
        Preconditions.checkNotNull(positionFilePath);

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, metaDir={}",
                    new Object[]{ReliableTaildirEventReader.class.getSimpleName(), filePaths});
        }

        List<TaildirMatcher> taildirCache = Lists.newArrayList();
        for (Entry<String, String> e : filePaths.entrySet()) {
            taildirCache.add(new TaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));
        }
        logger.info("taildirCache: " + taildirCache.toString());
        logger.info("headerTable: " + headerTable.toString());

        this.taildirCache = taildirCache;
        this.headerTable = headerTable;
        this.addByteOffset = addByteOffset;
        this.cachePatternMatching = cachePatternMatching;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;
        this.isFileHostEnable =isFileHostEnable;
        if (!"".equals(dateFileFormat) && !"".equals(dateHeaderFormat)) {
            this.useFileTimeFormat(dateFileFormat, dateHeaderFormat);
        }
        updateTailFiles(skipToEnd);

        logger.info("Updating position from position file: " + positionFilePath);
        loadPositionFile(positionFilePath);
    }

    public ReliableTaildirEventReader useFileTimeFormat(String dateFormat, String dateHeaderFormat) {
        this.format = dateFormat;
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat);
        this.dateHeaderFormatter = DateTimeFormatter.ofPattern(dateHeaderFormat);
        this.enableGetFileTime();
        return this;
    }

    private void enableGetFileTime() {
        isFileTimeEnable = true;
    }

    /**
     * Load a position file which has the last read position of each file.
     * If the position file exists, update tailFiles mapping.
     */
    public void loadPositionFile(String filePath) {
        Long inode, pos;
        String path;
        FileReader fr = null;
        JsonReader jr = null;
        try {
            fr = new FileReader(filePath);
            jr = new JsonReader(fr);
            jr.beginArray();
            while (jr.hasNext()) {
                inode = null;
                pos = null;
                path = null;
                jr.beginObject();
                while (jr.hasNext()) {
                    switch (jr.nextName()) {
                        case "inode":
                            inode = jr.nextLong();
                            break;
                        case "pos":
                            pos = jr.nextLong();
                            break;
                        case "file":
                            path = jr.nextString();
                            break;
                    }
                }
                jr.endObject();

                for (Object v : Arrays.asList(inode, pos, path)) {
                    Preconditions.checkNotNull(v, "Detected missing value in position file. "
                            + "inode: " + inode + ", pos: " + pos + ", path: " + path);
                }
                TailFile tf = tailFiles.get(inode);
                if (tf != null && tf.updatePos(path, inode, pos)) {
                    tailFiles.put(inode, tf);
                } else {
                    logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
                }
            }
            jr.endArray();
        } catch (FileNotFoundException e) {
            logger.info("File not found: " + filePath + ", not updating position");
        } catch (IOException e) {
            logger.error("Failed loading positionFile: " + filePath, e);
        } finally {
            try {
                if (fr != null) fr.close();
                if (jr != null) jr.close();
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }

    public Map<Long, TailFile> getTailFiles() {
        return tailFiles;
    }

    public void setCurrentFile(TailFile currentFile) {
        this.currentFile = currentFile;
    }

    @Override
    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (events.isEmpty()) {
            return null;
        }
        return events.get(0);
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        return readEvents(numEvents, false);
    }

    @VisibleForTesting
    public List<Event> readEvents(TailFile tf, int numEvents) throws IOException {
        setCurrentFile(tf);
        return readEvents(numEvents, true);
    }

    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
            throws IOException {
        if (!committed) {
            if (currentFile == null) {
                throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
            }
            logger.info("Last read was never committed - resetting position");
            long lastPos = currentFile.getPos();
            currentFile.updateFilePos(lastPos);
        }
        List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset);
        if (events.isEmpty()) {
            return events;
        }

        Map<String, String> headers = currentFile.getHeaders();
        if (annotateFileName || (headers != null && !headers.isEmpty())) {
            for (Event event : events) {
                if (headers != null && !headers.isEmpty()) {
                    event.getHeaders().putAll(headers);
                }
                if (annotateFileName) {
                    event.getHeaders().put(fileNameHeader, currentFile.getPath());
                }
            }
        }
        committed = false;
        return events;
    }


    @Override
    public void close() throws IOException {
        for (TailFile tf : tailFiles.values()) {
            if (tf.getRaf() != null) tf.getRaf().close();
        }
    }

    /**
     * Commit the last lines which were read.
     */
    @Override
    public void commit() throws IOException {
        if (!committed && currentFile != null) {
            long pos = currentFile.getLineReadPos();
            currentFile.setPos(pos);
            currentFile.setLastUpdated(updateTime);
            committed = true;
        }
    }

    /*
        从文件名中截取host
     */
    private static final String hostSplitor = "-";

    private String getHostStr(String fileName) {
        int index = fileName.indexOf(hostSplitor);
        String host = fileName.substring(0, index);
        return host;
    }


    private String getDateStr(String fileName) {//从文件名里截日期
        int length = fileName.length();
        int dateLength = this.format.length();
        int start = length - dateLength;
        String dateStr = fileName.substring(start);
        return dateStr;
    }

    /**
     * Update tailFiles mapping if a new file is created or appends are detected
     * to the existing file.
     */
    public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
        updateTime = System.currentTimeMillis();//懂了，每次（查找是否有新的文件或者（旧的文件新的内容的时候才会更新））
        List<File> files = Lists.newArrayList();
        for (TaildirMatcher taildir : taildirCache) {

            Map<String, String> headers = headerTable.row(taildir.getFileGroup());

            for (File f : taildir.getMatchingFiles()) {
                HashMap<String, String> fileHeader = new HashMap<>();
                fileHeader.putAll(headers);
                String fileName = f.getName();
                if (this.isFileTimeEnable) { //可以从文件名中截取时间
                    String dateStr = this.getDateStr(fileName);
                    LocalDateTime date = LocalDateTime.parse(dateStr, this.dateTimeFormatter);
                    String hourHeader = String.format("%02d",date.getHour());
                    String dateHeader = this.dateHeaderFormatter.format(date);
                    fileHeader.put(HOUR_HEADER, hourHeader);
                    fileHeader.put(DATE_HEADER, dateHeader);
                }
                if (this.isFileHostEnable) {//可以从文件名中截取域名,设这个的前提是丫你的文件名前缀是 host-
                    String hostStr = this.getHostStr(fileName);
                    fileHeader.put(HOST_HEADER, hostStr);
                }

                long inode = getInode(f);
                TailFile tf = tailFiles.get(inode);
                if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
                    long startPos = skipToEnd ? f.length() : 0;
                    tf = openFile(f, fileHeader, inode, startPos);
                } else {
                    boolean updated = tf.getLastUpdated() < f.lastModified() || tf.getPos() != f.length();
                    if (updated) {
                        if (tf.getRaf() == null) {
                            tf = openFile(f, fileHeader, inode, tf.getPos());
                        }
                        if (f.length() < tf.getPos()) {
                            logger.info("Pos " + tf.getPos() + " is larger than file size! "
                                    + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
                            tf.updatePos(tf.getPath(), inode, 0);
                        }
                    }
                    tf.setNeedTail(updated);
                }
                tailFiles.put(inode, tf);
                files.add(f);
            }
        }
        List<Long> result = this.sortedByCreateTime(files);
        return result;

    }

    protected List<Long> sortedByCreateTime(List<File> files) {
        Map<File, Long> fileCreateTime =
                files.stream()  //找到文件和createTime的映射(优化排序。要不每回排序都要搜一下createTime);
                        .collect(Collectors.toMap(s -> s, ReliableTaildirEventReader.this::getCreateTime));
        List<Long> result = files.stream()
                .sorted((o1, o2) -> fileCreateTime.get(o1).compareTo(fileCreateTime.get(o2)))//createTime从小到大排序
                .map(file -> {  //获取inode
                    long inode = -1;
                    try {
                        inode = this.getInode(file);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return inode;
                })
                .filter(inode -> inode > 0)//滤掉有用的inode//装箱
                .collect(Collectors.toList());
        return result;
    }

    public long getCreateTime(File file) {
        Path path = Paths.get(file.getAbsolutePath());
        long createTime = 0;
        try {
            BasicFileAttributes basicFileAttributes = Files.readAttributes(path, BasicFileAttributes.class);
            createTime = basicFileAttributes.creationTime().toMillis();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return createTime;
    }

    public List<Long> updateTailFiles() throws IOException {
        return updateTailFiles(false);
    }


    private long getInode(File file) throws IOException {
        long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
        return inode;
    }

    private TailFile openFile(File file, Map<String, String> headers, long inode, long pos) {
        try {
            logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos);
            return new TailFile(file, headers, inode, pos);
        } catch (IOException e) {
            throw new FlumeException("Failed opening file: " + file, e);
        }
    }

    /**
     * Special builder class for ReliableTaildirEventReader
     */
    public static class Builder {
        private Map<String, String> filePaths;
        private Table<String, String, String> headerTable;
        private String positionFilePath;
        private boolean skipToEnd;
        private boolean addByteOffset;
        private boolean cachePatternMatching;
        private String dateFileFormat;
        private String dateHeaderFormat;
        private boolean isFileHostEnable;
        private Boolean annotateFileName =
                TaildirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
        private String fileNameHeader =
                TaildirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;

        public Builder filePaths(Map<String, String> filePaths) {
            this.filePaths = filePaths;
            return this;
        }
        public Builder isFileHostEnable(boolean isFileHostEnable){
            this.isFileHostEnable =isFileHostEnable;
            return this;
        }

        public Builder headerTable(Table<String, String, String> headerTable) {
            this.headerTable = headerTable;
            return this;
        }

        public Builder positionFilePath(String positionFilePath) {
            this.positionFilePath = positionFilePath;
            return this;
        }

        public Builder skipToEnd(boolean skipToEnd) {
            this.skipToEnd = skipToEnd;
            return this;
        }

        public Builder addByteOffset(boolean addByteOffset) {
            this.addByteOffset = addByteOffset;
            return this;
        }

        public Builder cachePatternMatching(boolean cachePatternMatching) {
            this.cachePatternMatching = cachePatternMatching;
            return this;
        }

        public Builder annotateFileName(boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public Builder dateFileFormat(String dateFileFormat) {
            this.dateFileFormat = dateFileFormat;
            return this;
        }

        public Builder dateHeaderFormat(String dateHeaderFormat) {
            this.dateHeaderFormat = dateHeaderFormat;
            return this;
        }

        public ReliableTaildirEventReader build() throws IOException {
            return new ReliableTaildirEventReader(filePaths, headerTable, positionFilePath, skipToEnd,
                    addByteOffset, cachePatternMatching,
                    annotateFileName, fileNameHeader, dateFileFormat, dateHeaderFormat,isFileHostEnable);
        }
    }

}
