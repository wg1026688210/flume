package org.apache.flume.source.taildir;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SortFilesTest{
    @Test
    public void test(){
        ArrayList<File> files = new ArrayList<File>() {
            {
                this.add(new File("/tmp/test/a/5"));
                this.add(new File("/tmp/test/q/1"));
                this.add(new File("/tmp/test/w/2"));
                this.add(new File("/tmp/test/e/3"));
                this.add(new File("/tmp/test/r/4"));
            }
        };
        List<File> files1 = this.sortedByCreateTime(files);
        System.out.println(files);
    }
    protected List<File> sortedByCreateTime(List<File> files) {
        Map<File, Long> fileCreateTime =
                files.stream()  //找到文件和createTime的映射(优化排序。要不每回排序都要搜一下createTime);
                        .collect(Collectors.toMap(s -> s, this::getCreateTime));
        List<File> collect = files.stream()
                .sorted((o1, o2) -> fileCreateTime.get(o1).compareTo(fileCreateTime.get(o2)))//createTime从小到大排序
                .collect(Collectors.toList());
        return collect;
    }
    public long getCreateTime(File file) {
        String absolutePath = file.getAbsolutePath();
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
    private long getInode(File file) throws IOException {
        long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
        return inode;
    }
}
