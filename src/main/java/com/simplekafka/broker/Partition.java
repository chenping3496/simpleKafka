package com.simplekafka.broker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Partition {
    public static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024;  // 1MB 滚段
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());

    private final int id;                    // 分区号
    private int leader;                      // leader broker id（第5课 rebalance 会改 → 故意非 final）
    private List<Integer> followers;         // follower id 列表（同上）
    private final String baseDir;            // 本分区的磁盘目录
    private final AtomicLong nextOffset;     // 下一条消息的编号：多线程 append 时的"发号器"
    private final ReadWriteLock lock;        // 读写锁：append 独占，read 共享
    private RandomAccessFile activeLogFile;  // 当前活跃段（正在写的那个）
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments; // 所有段，按 baseOffset 升序


    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        initialize();
    }

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public List<Integer> getFollowers() {
        return followers;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public AtomicLong getNextOffset() {
        return nextOffset;
    }

    public ReadWriteLock getLock() {
        return lock;
    }

    public RandomAccessFile getActiveLogFile() {
        return activeLogFile;
    }

    public FileChannel getActiveLogChannel() {
        return activeLogChannel;
    }

    public List<SegmentInfo> getSegments() {
        return segments;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public void setFollowers(List<Integer> followers) {
        this.followers = followers;
    }

    private void initialize() {
        try {
            File dir = new File(baseDir);
            if (!dir.exists()) dir.mkdirs();

            // 扫描目录里所有 .log 文件，配对出段（有 .log 无 .index 的残段不要）
            File[] files = dir.listFiles((d, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null) {
                for (File file : files) {
                    String baseName = file.getName().replace(LOG_SUFFIX, "");   // "000...05"
                    long baseOffset = Long.parseLong(baseName);                 // 段的起点
                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        segments.add(new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath()));
                    }
                }
            }
            segments.sort(Comparator.comparingLong(SegmentInfo::getBaseOffset));  // 按起点排序！

            if (segments.isEmpty()) {
                createNewSegment(0);                         // 全新分区：从 0 开始
            } else {
                SegmentInfo last = segments.get(segments.size() - 1);
                openSegmentForAppend(last);                  // 老分区：打开最后一段继续写
                nextOffset.set(last.getBaseOffset() + countMessagesInSegment(last));  // ★ 恢复关键步
            }
            LOGGER.info("Partition " + id + " 恢复完成，nextOffset=" + nextOffset.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
    }

    public long append(byte[] message) {
        lock.writeLock().lock();
        try {
            long currentOffset = nextOffset.get();          // ① 先取号

            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {   // ② 段满了
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);            //    以"我这条的offset"命名新段
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);  // ③ 打包 [4B len][msg]
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            long position = activeLogChannel.position();    // ④ 记住写入位置（索引要用）
            activeLogChannel.write(buffer);
            activeLogChannel.force(true);                   // ⑤ 强制落盘：不依赖OS缓存

            updateIndex(currentOffset, position);           // ⑥ 索引记一笔
            nextOffset.incrementAndGet();                   // ⑦ 发下一个号
            return currentOffset;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append to partition " + id, e);
            return -1;                                      // 惯例：-1 表示失败
        } finally {
            lock.writeLock().unlock();                      // 铁律：finally 里解锁
        }
    }

/**
 * 更新索引文件的方法，将当前偏移量和位置信息写入索引文件的末尾
 *
 * @param currentOffset 当前偏移量
 * @param position 位置信息
 */
    private void updateIndex(long currentOffset, long position) {
    // 使用try-with-resources语句确保资源正确关闭
        try (RandomAccessFile indexFile = new RandomAccessFile(segments.get(segments.size() - 1).getIndexPath(), "rw");
             FileChannel indexChannel = indexFile.getChannel()) {
        // 创建包含当前偏移量和位置信息的字节数组
        // 并将其包装为ByteBuffer
        // 然后将数据写入到索引文件的当前末尾位置
            indexChannel.position(indexChannel.size());
            ByteBuffer buffer = ByteBuffer.allocate(16);  // ③ 打包 [4B len][msg]
            buffer.putLong(currentOffset);
            buffer.putLong(position);
            buffer.flip();
            indexChannel.write(buffer);
            indexChannel.force(true);
        } catch (IOException e) {
        // 记录错误日志，包含分区ID和异常信息
            LOGGER.log(Level.SEVERE, "Failed to update index for partition " + id, e);
        }
    }


    /**
 * 计算指定日志段中的消息数量
 * @param segment 日志段信息对象，包含日志路径等元数据
 * @return 该日志段中包含的有效消息数量
 * @throws IOException 如果在读取文件时发生I/O错误
 */
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0; // 消息计数器，初始化为0
    // 使用try-with-resources确保文件资源被正确关闭
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel ch = logFile.getChannel()) {
        // 分配4字节的缓冲区，用于读取消息长度字段
            ByteBuffer sizeBuf = ByteBuffer.allocate(4);
        // 遍历日志文件，直到文件末尾
            while (ch.position() < ch.size()) {
                sizeBuf.clear(); // 清空缓冲区，准备读取新的长度值
            // 读取消息长度字段
                if (ch.read(sizeBuf) < 4) break;     // 不足4B：尾部残缺（崩溃留下的半条），跳过
                sizeBuf.flip(); // 切换为读模式
                int msgSize = sizeBuf.getInt(); // 获取消息的实际长度
                ch.position(ch.position() + msgSize); // 跳过消息体，落在下一条的长度字段上
                count++;
            }
        }
        return count;
    }

/**
 * 从指定偏移量开始读取消息，最多读取maxBytes字节的数据
 * @param offset 开始读取的偏移量
 * @param maxBytes 最多读取的字节数
 * @return 包含读取到的消息的列表，每个消息是一个byte数组
 */
public List<byte[]> readMessages(long offset, int maxBytes) {
    // 获取读锁，保证线程安全
    lock.readLock().lock();
    // 初始化消息列表和已读取字节数
    List<byte[]> messages = new ArrayList<>();
    int bytesRead = 0;
    RandomAccessFile currentFile = null;

    try {
        // ---- ① 定位起点段（返回下标版，见下）----
        // 查找包含指定偏移量的段索引
        int segmentIndex = findSegmentIndexForOffset(offset);
        if (segmentIndex < 0) return messages;    // 越界处理

        // 在段内查找具体位置
        long position = findPositionForOffset(segments.get(segmentIndex), offset);
        if (position < 0) return messages;

        // ---- ② 外层：一段一段往后走 ----
        // 遍历所有段，直到读取完足够数据或到达末尾
        while (segmentIndex < segments.size() && bytesRead < maxBytes) {
            SegmentInfo segment = segments.get(segmentIndex);

            // 换段的第一轮（或第一次进入）：打开文件、seek 到起始位置
            if (currentFile != null) currentFile.close();          // 旧笔封掉
            currentFile = new RandomAccessFile(segment.getLogPath(), "r");
            FileChannel currentChannel = currentFile.getChannel();
            currentChannel.position(position);                     // 首轮是索引给的位置；后续轮是 0

            // ---- 内层：在当前段里读消息 ----
            ByteBuffer sizeBuf = ByteBuffer.allocate(4);
            while (bytesRead < maxBytes && currentChannel.position() < currentChannel.size()) {
                sizeBuf.clear();
                if (currentChannel.read(sizeBuf) < 4) break;       // 段尾残缺
                sizeBuf.flip();
                int messageSize = sizeBuf.getInt();

                if (bytesRead + messageSize > maxBytes) break;     // 预算不够

                ByteBuffer msgBuf = ByteBuffer.allocate(messageSize);
                if (currentChannel.read(msgBuf) < messageSize) break;
                msgBuf.flip();
                messages.add(Arrays.copyOf(msgBuf.array(), messageSize));

                bytesRead += messageSize + 4;
            }

            // ---- ③ 决定下一步 ----
            if (bytesRead >= maxBytes) break;                      // 预算用完，收工
            if (currentChannel.position() < currentChannel.size()) break;
            //   ↑ 本段还没读完就退出了内层循环 = 预算用完，同样收工

            // 真正的"本段读完还有得读"：切下一段
            segmentIndex++;
            position = 0;                        // ★ 新段从头读——这就是 TODO 的核心
        }
    } catch (IOException e) {
        LOGGER.log(Level.SEVERE, "Failed to read partition " + id, e);
    } finally {
        try {
            if (currentFile != null) currentFile.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to close segment file", e);
        }
        lock.readLock().unlock();
    }
    return messages;
}

    private long findPositionForOffset(SegmentInfo segment, long offset) throws IOException {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
             FileChannel indexChannel = indexFile.getChannel()) {

            if (indexChannel.size() == 0) return 0;              // 空索引：从头读

            long relativeOffset = offset - segment.getBaseOffset();   // 段内第几条
            long entryCount = indexChannel.size() / 16;               // 每项16字节

            if (relativeOffset >= entryCount) {
                // ⚠ 防御：索引没记到这个 offset（重启恢复边界情况）→ 用最后一项的位置兜底
                indexChannel.position(indexChannel.size() - 16);
                ByteBuffer buf = ByteBuffer.allocate(16);
                indexChannel.read(buf);
                buf.flip();
                buf.getLong();                       // 跳过 offset 字段
                return buf.getLong();                // 返回 position 字段
            }

            indexChannel.position(relativeOffset * 16);   // ★ 直接跳到第 relativeOffset 项——索引的意义
            ByteBuffer buf = ByteBuffer.allocate(16);
            indexChannel.read(buf);
            buf.flip();
            buf.getLong();                               // 跳过 offset（校验用，教学版略）
            return buf.getLong();
        }
    }

    /** 返回包含 offset 的段的下标；越界返回 -1 */
    private int findSegmentIndexForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) return -1;
        int result = -1;
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i).getBaseOffset() <= offset) result = i;
            else break;
        }
        return result;
    }
    public long getLogEndOffset() {          // 改 public：第5课 broker 要调它判断 FETCH 越界
        return nextOffset.get();             // 一行。AtomicLong 已发的号就是日志末尾
    }

    public void close(){
        try {
            activeLogChannel.close();
            activeLogFile.close();
        } catch (IOException e){
            LOGGER.log(Level.SEVERE, "Failed to close partition " + id, e);
        }
    }

    //===================================Segment相关操作=====================================
    private class SegmentInfo {
        long baseOffset;
        String logPath;
        String indexPath;

        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public String getLogPath() {
            return logPath;
        }

        public String getIndexPath() {
            return indexPath;
        }
    }

    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);                  // 7 → "00000000000000000007"补零到20位
        String logPath  = baseDir + File.separator + baseName + LOG_SUFFIX;    // data/1/order/0/000...07.log
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX; // data/1/order/0/000...07.index

        new File(logPath).createNewFile();     // 真的在磁盘上把两个空文件建出来
        new File(indexPath).createNewFile();

        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath); // ★ 就在这里填进去
        segments.add(segment);                 // 登记进账本列表

        openSegmentForAppend(segment);         // 笔尖切到新段（activeLogFile/Channel 指向它，position=0）
    }

    //笔尖切到新段（activeLogFile/Channel 指向它，position=0）
    private void openSegmentForAppend(SegmentInfo segment) {
        try {
            activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
            activeLogChannel = activeLogFile.getChannel();
            activeLogChannel.position(activeLogChannel.size()); // position 切到文件末尾
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to open segment for append", e);}
    }

    public static void main(String[] args) throws Exception {
        String dir = "E:/progrmmes/simpleKafka/testdata/order/0";
        Partition p = new Partition(0, 1, List.of(2, 3), dir);

        System.out.println(p.append("hello".getBytes()));   // 0
        System.out.println(p.append("world".getBytes()));   // 1
        System.out.println(p.append("kafka".getBytes()));   // 2

        List<byte[]> all = p.readMessages(0, 1024);
        System.out.println(all.size());                     // 3
        System.out.println(new String(all.get(2)));         // kafka（从0读的第3条）

        List<byte[]> tail = p.readMessages(1, 1024);
        System.out.println(tail.size() + " " + new String(tail.get(0)));  // 2 world（从中间读）

        p.close();

        // ★ 重启恢复：新对象指向同一目录——本课的验收灵魂
        Partition p2 = new Partition(0, 1, List.of(2, 3), dir);
        System.out.println(p2.append("after-restart".getBytes()));         // 3 ← 接着上次的号发！
        System.out.println(new String(p2.readMessages(0, 4096).get(3)));   // after-restart
        p2.close();
    }
}
