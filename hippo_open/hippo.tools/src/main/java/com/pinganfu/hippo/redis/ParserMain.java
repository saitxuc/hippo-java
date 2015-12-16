package com.pinganfu.hippo.redis;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.pinganfu.hippo.client.HippoClient;
import com.pinganfu.hippo.client.HippoConnector;
import com.pinganfu.hippo.client.HippoResult;
import com.pinganfu.hippo.client.impl.HippoClientImpl;
import com.pinganfu.hippo.redis.paser.RDBPaser;
import com.pinganfu.hippo.redis.paser.RedisConstants;
import com.pinganfu.hippo.redis.paser.RedisEntry;

public class ParserMain {
    private static final Logger LOG = LoggerFactory.getLogger(ParserMain.class);
    static AtomicLong allCount = new AtomicLong(0);
    static AtomicLong expireCount = new AtomicLong(0);
    static AtomicLong successCount = new AtomicLong(0);
    static AtomicLong failedCount = new AtomicLong(0);

    public static void main(String[] args) {
        //get the file
        ExecutorService service = new ThreadPoolExecutor(18, 18, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "SubmitThread");
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());

        List<FileOutputStream> outputStreams = new ArrayList<FileOutputStream>();
        List<BufferedOutputStream> bufferedOutputStreams = new ArrayList<BufferedOutputStream>();

        if (args.length < 4) {
            LOG.error("the params not right!! 1.filePath 2.hippo-cclusterName 3.hippo's zk address 4.needOutFile needed");
            return;
        }

        String path = args[0];

        final String cclusterName = args[1];

        final String zkAddress = args[2];

        final String needOutFile = args[3];

        long beginTime = System.currentTimeMillis();

        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(cclusterName) || StringUtils.isEmpty(zkAddress)) {
            LOG.error("the params not right!! 1.filePath 2.hippo-cclusterName 3.hippo's zk address needed");
            return;
        }

        if (Boolean.parseBoolean(needOutFile)) {
            LOG.warn("out put file is not needed!!");
        }

        File outPutFile = null;

        File temp = new File("");
        outPutFile = new File(temp.getAbsolutePath() + File.separator + "out");

        if (!outPutFile.exists()) {
            outPutFile.mkdir();
        }

        if (!outPutFile.isDirectory()) {
            LOG.error("outPutFile -> " + outPutFile + " should be directory!!");
            return;
        }

        File filePath = new File(path);

        LOG.info("will parser the files under the folder -> " + path);
        File[] files = getFiles(filePath);

        if (files.length <= 0) {
            LOG.error("could not find the .rdb files, the program will stop!!");
            return;
        }

        final HippoClient client = getHippoClient(cclusterName, zkAddress);
        client.start();

        final Gson gson = new Gson();

        try {
            for (File file : files) {
                if (!file.getName().endsWith(".rdb")) {
                    LOG.info("file is not right!! the file's suffix should .rdb , the actually name is -> " + file.getName());
                    return;
                }

                LOG.info("will parser the file -> " + path + File.separator + file);

                RDBPaser paser = new RDBPaser();
                paser.init(file);

                if (!paser.verifyMagicString() || !paser.verifyVersion()) {
                    runtimeError("ERROR:the file is not redis dump, will be ignore!! -> " + file.getName());
                    continue;
                }

                File outFile = new File(outPutFile.getAbsolutePath() + File.separator + file.getName() + ".out");

                if (outFile.exists()) {
                    outFile.delete();
                }

                outFile.createNewFile();
                final FileOutputStream fileOutputStream = new FileOutputStream(outFile);
                outputStreams.add(fileOutputStream);
                final BufferedOutputStream boutStream = new BufferedOutputStream(fileOutputStream);
                bufferedOutputStreams.add(boutStream);

                try {
                    while (true) {
                        final RedisEntry entry = paser.loadEntry();

                        if (entry != null && entry.getType() == RedisConstants.REDIS_RDB_OPCODE_EOF) {
                            LOG.info("detect the file end, read for single rbd is done...");
                            break;
                        } else if (entry == null) {
                            LOG.info("WARN: entry is null, read for single rbd is done....");
                            break;
                        }

                        allCount.incrementAndGet();

                        if (allCount.get() % 1000 == 0) {
                            LOG.info(new Date() + " -> has been finished count : " + allCount.get() + " , expire count : " + expireCount.get() + " , success count :" + successCount
                                .get() + " , fail count : " + failedCount.get());
                        }

                        service.execute(new Runnable() {
                            @Override
                            public void run() {
                                int expire = 0;
                                boolean isPer = false;
                                if (entry.getExpire() == 0) {
                                    isPer = true;
                                } else {
                                    expire = (int) ((entry.getExpire() - System.currentTimeMillis()) / 1000);
                                }

                                if (entry.getValue() instanceof Serializable) {
                                    if (!isPer && expire <= 0) {
                                        System.out.println("WARN:" + entry.getKey() + " has expired!");
                                        expireCount.incrementAndGet();
                                        return;
                                    }

                                    for (int i = 0; i < 5; i++) {
                                        HippoResult result = client.set(expire, entry.getKey(), (Serializable) entry.getValue(), 0);
                                        if (result.isSuccess()) {
                                            //progress
                                            successCount.incrementAndGet();

                                            //output the key & value to the txt
                                            if (Boolean.parseBoolean(needOutFile)) {
                                                String val = gson.toJson(entry.getValue());
                                                synchronized (boutStream) {
                                                    try {
                                                        boutStream.write((entry.getType() + " :: " + entry.getKey() + " -> " + val + "\n").getBytes());
                                                        boutStream.flush();
                                                    } catch (Exception e) {
                                                        LOG.error("entry out put error!!key -> " + entry.getKey(), e);
                                                    }
                                                }
                                            }

                                            break;
                                        } else {
                                            if (i == 4) {
                                                LOG.error("key -> " + entry.getKey() + " has failed!!!");
                                                failedCount.incrementAndGet();
                                            } else {
                                                LOG.warn("key -> " + entry.getKey() + " will try again................");
                                            }
                                        }
                                    }
                                } else {
                                    LOG.error("key -> " + entry.getKey() + " the value could not be Serializable!!, this will be ignored!");
                                }
                            }
                        });
                    }
                } catch (Exception e1) {
                    //e1.printStackTrace();
                    LOG.error("", e1);
                } finally {
                    paser.close();
                }
            }

            service.shutdown();

            while (true) {
                if (service.awaitTermination(10, TimeUnit.SECONDS)) {
                    long endTime = System.currentTimeMillis();
                    LOG.info("all has been done , cost time is  : " + (endTime - beginTime) / 1000 + " second , expire count : " + expireCount.get() + " , success count :" + successCount
                        .get() + " , fail count : " + failedCount.get() + " ,all count :" + allCount.get() + "!!\n\n");
                    break;
                }
            }

            for (BufferedOutputStream boutStream : bufferedOutputStreams) {
                if (boutStream != null) {
                    boutStream.close();
                }
            }

            for (FileOutputStream fileOutputStream : outputStreams) {
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.stop();
            }
        }
    }

    private static File[] getFiles(File filePath) {
        if (filePath.isDirectory()) {
            File[] result = filePath.listFiles();
            if (result.length <= 0) {
                LOG.error("could not find the .rdb files, the program will stop!!");
                return null;
            } else {
                return result;
            }
        } else {
            File[] result = new File[1];
            result[0] = filePath;
            return result;
        }
    }

    private static void runtimeError(String msg, Object... args) {
        LOG.error(String.format(msg, args));
        throw new RuntimeException(String.format(msg, args));
    }

    private static HippoClient getHippoClient(String cclusterName, String zkAddress) {
        final HippoConnector hippoConnector = new HippoConnector();
        hippoConnector.setClusterName(cclusterName);
        hippoConnector.setZookeeperUrl(zkAddress);
        hippoConnector.setSessionInstance(25);
        HippoClientImpl client = new HippoClientImpl(hippoConnector);
        return client;
    }
}
