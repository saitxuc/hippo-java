package com.hippo.client.impl;

import com.hippo.client.*;
import com.hippo.client.command.*;
import com.hippo.client.exception.HippoClientException;
import com.hippo.client.transport.AbstractClientConnectionControl;
import com.hippo.client.util.ClientSPIManager;
import com.hippo.common.errorcode.HippoCodeDefine;
import com.hippo.common.exception.HippoException;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.common.serializer.KryoSerializer;
import com.hippo.common.serializer.Serializer;
import com.hippo.common.util.ByteUtil;
import com.hippo.common.util.KeyUtil;
import com.hippo.common.util.Logarithm;
import com.hippo.network.Session;
import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.Response;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author saitxuc write 2014-8-11
 */
public class HippoClientImpl extends LifeCycleSupport implements HippoClient {

    private static final Logger log = LoggerFactory.getLogger(HippoClientImpl.class);

    private long request_timeout = 3000;

    protected HippoConnector connector;

    protected AbstractClientConnectionControl connectionControl;

    private String protocal;

    private Serializer serializer = null;

    private ExecutorService service = Executors.newFixedThreadPool(5);

    private final int defaultVal = 32 * 1024;

    private byte[] separator = null;

    public HippoClientImpl() {
        this.serializer = new KryoSerializer();
    }

    public HippoClientImpl(HippoConnector connector) {
        this();
        this.protocal = connector.getScheme();
        this.connector = connector;
    }

    @Override
    public HippoResult get(Serializable key) {
        Session session = null;
        byte[] keybytes = null;
        try {
            GetCommand command = new GetCommand();
            keybytes = serializer.serialize(key);
            command.setData(keybytes);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());

            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), null);
            } else {
                String versionStr = rsp.getHeadValue("version");
                String expireTimeStr = rsp.getHeadValue("expireTime");
                int version = 0;
                long expireTime = 0;
                if (!StringUtils.isEmpty(versionStr)) {
                    version = Integer.parseInt(versionStr);
                }
                if (!StringUtils.isEmpty(expireTimeStr)) {
                    expireTime = Long.parseLong(expireTimeStr);
                }

                if (expireTime > 0 && System.currentTimeMillis() > expireTime) {
                    result = new HippoResult(false, null, 0);
                } else {
                    result = new HippoResult(true, rsp.getData(), version);
                }
            }
            return result;
        } catch (IOException e) {
            log.error("get vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("get vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public <K, V> HippoResult set(int expire, Serializable key, Map<K, V> value, int version) {
        Session session = null;
        byte[] keybytes = null;
        byte[] valuebytes = null;
        try {
            keybytes = serializer.serialize(key);
            valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            Command command = createSetOrUpdateCommand(expire, keybytes, valuebytes, version);
            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }

            return result;
        } catch (IOException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public <T> HippoResult set(int expire, Serializable key, Set<T> value, int version) {
        Session session = null;
        byte[] keybytes = null;
        byte[] valuebytes = null;
        try {
            keybytes = serializer.serialize(key);
            valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            Command command = createSetOrUpdateCommand(expire, keybytes, valuebytes, version);
            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }

            return result;
        } catch (IOException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public <T> HippoResult set(int expire, Serializable key, List<T> value, int version) {
        Session session = null;
        byte[] keybytes = null;
        byte[] valuebytes = null;
        try {
            keybytes = serializer.serialize(key);
            valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            Command command = createSetOrUpdateCommand(expire, keybytes, valuebytes, version);
            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }

            return result;
        } catch (IOException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public HippoResult set(int expire, Serializable key, Serializable value, int version) {
        Session session = null;
        byte[] keybytes = null;
        byte[] valuebytes = null;
        try {
            keybytes = serializer.serialize(key);
            valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            Command command = createSetOrUpdateCommand(expire, keybytes, valuebytes, version);
            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }

            return result;
        } catch (IOException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public HippoResult set(int expire, Serializable key, Serializable value) {
        return this.set(expire, key, value, 0);
    }

    @Override
    public HippoResult set(Serializable key, Serializable value) {
        Session session = null;
        byte[] keybytes = null;
        try {
            SetCommand command = new SetCommand();
            keybytes = serializer.serialize(key);
            byte[] valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            byte[] databytes = new byte[keybytes.length + valuebytes.length];
            System.arraycopy(keybytes, 0, databytes, 0, keybytes.length);
            System.arraycopy(valuebytes, 0, databytes, keybytes.length, valuebytes.length);
            command.setKlength(keybytes.length);
            command.setData(databytes);
            command.setExpire(-1);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, "0");
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("set vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public HippoResult update(int expire, Serializable key, Serializable value, int version) {
        Session session = null;
        byte[] keybytes = null;
        try {
            UpdateCommand command = new UpdateCommand();
            keybytes = serializer.serialize(key);
            byte[] valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            byte[] databytes = new byte[keybytes.length + valuebytes.length];
            System.arraycopy(keybytes, 0, databytes, 0, keybytes.length);
            System.arraycopy(valuebytes, 0, databytes, keybytes.length, valuebytes.length);
            command.setKlength(keybytes.length);
            command.setData(databytes);
            command.setExpire(expire);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public HippoResult update(int expire, Serializable key, Serializable value) {
        return this.update(expire, key, value, 0);
    }

    @Deprecated
    @Override
    public HippoResult update(Serializable key, Serializable value) {
        return this.update(-1, key, value, 0);
    }

    @Override
    public <T> HippoResult update(int expire, Serializable key, List<T> value, int version) {
        Session session = null;
        byte[] keybytes = null;
        try {
            UpdateCommand command = new UpdateCommand();
            keybytes = serializer.serialize(key);
            byte[] valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            byte[] databytes = new byte[keybytes.length + valuebytes.length];
            System.arraycopy(keybytes, 0, databytes, 0, keybytes.length);
            System.arraycopy(valuebytes, 0, databytes, keybytes.length, valuebytes.length);
            command.setKlength(keybytes.length);
            command.setData(databytes);
            command.setExpire(expire);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public <T> HippoResult update(int expire, Serializable key, List<T> value) {
        return this.update(expire, key, value, 0);
    }

    @Deprecated
    @Override
    public <T> HippoResult update(Serializable key, List<T> value) {
        return this.update(-1, key, value, 0);
    }

    @Override
    public <T> HippoResult update(int expire, Serializable key, Set<T> value, int version) {
        Session session = null;
        byte[] keybytes = null;
        try {
            UpdateCommand command = new UpdateCommand();
            keybytes = serializer.serialize(key);
            byte[] valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            byte[] databytes = new byte[keybytes.length + valuebytes.length];
            System.arraycopy(keybytes, 0, databytes, 0, keybytes.length);
            System.arraycopy(valuebytes, 0, databytes, keybytes.length, valuebytes.length);
            command.setKlength(keybytes.length);
            command.setData(databytes);
            command.setExpire(expire);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public <T> HippoResult update(int expire, Serializable key, Set<T> value) {
        return this.update(expire, key, value, 0);
    }

    @Deprecated
    @Override
    public <T> HippoResult update(Serializable key, Set<T> value) {
        return this.update(-1, key, value, 0);
    }

    @Override
    public <K, V> HippoResult update(int expire, Serializable key, Map<K, V> value, int version) {
        Session session = null;
        byte[] keybytes = null;
        try {
            UpdateCommand command = new UpdateCommand();
            keybytes = serializer.serialize(key);
            byte[] valuebytes = serializer.serialize(value);
            checkDataSize((valuebytes.length + keybytes.length));
            byte[] databytes = new byte[keybytes.length + valuebytes.length];
            System.arraycopy(keybytes, 0, databytes, 0, keybytes.length);
            System.arraycopy(valuebytes, 0, databytes, keybytes.length, valuebytes.length);
            command.setKlength(keybytes.length);
            command.setData(databytes);
            command.setExpire(expire);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response rsp = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (rsp.isFailure()) {
                result = new HippoResult(false, rsp.getErrorCode(), rsp.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("update vaule happened error! ", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public <K, V> HippoResult update(int expire, Serializable key, Map<K, V> value) {
        return this.update(expire, key, value, 0);
    }

    @Deprecated
    @Override
    public <K, V> HippoResult update(Serializable key, Map<K, V> value) {
        return this.update(-1, key, value, 0);
    }

    @Deprecated
    @Override
    public HippoResult remove(int expire, Serializable key, int version) {
        Session session = null;
        byte[] keybytes = null;
        try {
            RemoveCommand command = new RemoveCommand();
            keybytes = serializer.serialize(key);
            command.setData(keybytes);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, String.valueOf(version));
            Response response = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (response.isFailure()) {
                result = new HippoResult(false, response.getErrorCode(), response.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("remove value happened error!", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("remove value happened error!", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Deprecated
    @Override
    public HippoResult remove(int expire, Serializable key) {
        return this.remove(expire, key, 0);
    }

    @Override
    public HippoResult remove(Serializable key) {
        Session session = null;
        byte[] keybytes = null;
        try {
            RemoveCommand command = new RemoveCommand();
            keybytes = serializer.serialize(key);
            command.setData(keybytes);

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());
            setVersioInCommand(command, "0");
            Response response = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (response.isFailure()) {
                result = new HippoResult(false, response.getErrorCode(), response.getContent());
            } else {
                result = new HippoResult(true);
            }
            return result;
        } catch (IOException e) {
            log.error("remove vaule happened error!", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("remove vaule happened error!", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public void doInit() {
        if (null == connector) {
            throw new IllegalStateException("connector is null, please check! ");
        }

        if (StringUtils.isEmpty(protocal)) {
            throw new IllegalArgumentException("protocal value is null, please check! ");
        }

        if (ClientConstants.TRANSPORT_PROTOCOL_CLUSTER.equals(protocal)) {
            if (StringUtils.isEmpty(connector.getClusterName())) {
                throw new IllegalStateException("Fail to create hippo client: cluster name is null");
            }
            if (StringUtils.isEmpty(connector.getZookeeperUrl())) {
                throw new IllegalStateException("Fail to create hippo client: zookeeper url is null");
            }
        } else {
            if (StringUtils.isEmpty(connector.getBrokerUrl())) {
                throw new IllegalStateException("Fail to create hippo client: broker url is null");
            }
        }
        try {
            connectionControl = ClientSPIManager.findConnectionControl(protocal);
            connectionControl.setConnector(connector);
        } catch (Exception e) {
            log.error("create ConnectionControl error!", e);
        }
    }

    @Override
    public void doStart() {
        connectionControl.start();
    }

    @Override
    public void doStop() {
        connectionControl.stop();
        serializer.close();
        service.shutdownNow();
    }

    private ClientSessionResult createSession(byte[] key) throws HippoClientException {
        return connectionControl.getSession(key);
    }

    /**
     * set bucketNum value to command
     *
     * @param command
     * @param bucketNum
     */
    private void setBucketNumInCommand(Command command, String bucketNum) {
        if (!StringUtils.isEmpty(bucketNum)) {
            command.putHeadValue(ClientConstants.HEAD_BUCKET_NO, bucketNum);
        }
    }

    private void setVersioInCommand(Command command, String version) {
        if (!StringUtils.isEmpty(version)) {
            command.putHeadValue(ClientConstants.HEAD_VERSION, version);
        }
    }

    public void setConnector(HippoConnector connector) {
        this.connector = connector;
        this.protocal = connector.getScheme();
    }

    @Override
    public HippoResult incr(int expire, Serializable key, long value, long defaultValue, boolean checkValEdge) {
        if (value < 0) {
            return new HippoResult(false);
        }

        return addCount(key, value, defaultValue, expire, checkValEdge);
    }

    @Override
    public HippoResult decr(int expire, Serializable key, long value, long defaultValue, boolean checkValEdge) {
        if (value < 0) {
            return new HippoResult(false);
        }

        return addCount(key, -value, defaultValue, expire, checkValEdge);
    }

    private HippoResult addCount(Serializable key, long value, long defaultValue, int expire, boolean checkValEdge) {
        Session session = null;
        byte[] keybytes = null;
        try {
            AtomicntCommand command = new AtomicntCommand();
            keybytes = serializer.serialize(key);
            command.setData(keybytes);
            command.setInitv(defaultValue);
            command.setDelta(value);
            command.setExpire(expire);
            command.putHeadValue("checkValEdge", checkValEdge + "");

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());

            Response response = (Response) session.send(command, request_timeout);
            HippoResult result = null;
            if (response.isFailure()) {
                result = new HippoResult(false, response.getErrorCode(), response.getContent());
            } else {
                result = new HippoResult(true);
                result.setData(response.getData());
            }
            return result;
        } catch (IOException e) {
            log.error("add count happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("add count happened error!", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public HippoResult incr(Serializable key, long value, long defaultValue) {
        return this.incr(-1, key, value, defaultValue, true);
    }

    @Override
    public HippoResult incr(Serializable key, long value) {
        return this.incr(-1, key, value, 0, true);
    }

    @Override
    public HippoResult incr(Serializable key) {
        return this.incr(-1, key, 1, 0, true);
    }

    @Override
    public HippoResult decr(Serializable key, long value, long defaultValue) {
        return this.decr(-1, key, value, defaultValue, true);
    }

    @Override
    public HippoResult decr(Serializable key, long value) {
        return this.decr(-1, key, value, Long.MAX_VALUE, true);
    }

    @Override
    public HippoResult decr(Serializable key) {
        return this.decr(-1, key, 1, Long.MAX_VALUE, true);
    }

    @Override
    public HippoResult getWholeBit(final Serializable key, int maxOffset, final int requestExpire, int timeOut) {

        byte[] keybytes = null;
        try {
            keybytes = serializer.serialize(key);
        } catch (IOException e) {
            log.error("serialize bit error when getWholeBit! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        }

        if (maxOffset < 0) {
            maxOffset = 100000001;
        } else {
            maxOffset++;
        }

        final AtomicInteger requestCount = new AtomicInteger(0);
        final AtomicInteger requestSuccess = new AtomicInteger(0);
        final AtomicInteger requestError = new AtomicInteger(0);
        final AtomicInteger requestReceive = new AtomicInteger(0);
        final int maxByte = (maxOffset % 8 == 0 ? maxOffset / 8 : maxOffset / 8 + 1);

        final int byteSizeLeft = KeyUtil.getByteSizeLeft(keybytes, CommandConstants.DEFAULT_BIT_BLOCKED_SIZE);
        int bitSizeLeft = byteSizeLeft * 8;
        int allBlockOffset = (maxOffset % bitSizeLeft == 0 ? maxOffset / bitSizeLeft : (maxOffset / bitSizeLeft + 1));

        final byte[] resultByte = new byte[maxByte];

        final CountDownLatch latch = new CountDownLatch(allBlockOffset);
        final byte[] originalKey = keybytes;
        for (int count = 0; count < allBlockOffset; count++) {
            final int currentOffsetEnd = (count + 1) * byteSizeLeft;
            final int currentOffsetBegin = (count) * byteSizeLeft;
            final byte[] newKey = KeyUtil.getKeyAfterCombineOffset(keybytes, Logarithm.intToBytes(currentOffsetEnd), CommandConstants.DEFAULT_BIT_OP_SEPRATOR);

            service.submit(new Runnable() {
                @Override
                public void run() {
                    Session session = null;
                    try {
                        GetBitCommand command = new GetBitCommand();
                        command.setData(newKey);
                        command.putHeadValue(CommandConstants.BIT_WHOLEGET, "true");
                        ClientSessionResult sessionResult = createSession(originalKey);

                        session = sessionResult.getSession();
                        setBucketNumInCommand(command, sessionResult.getBucketNum());

                        Response rsp = (Response) session.send(command, requestExpire * 1000);

                        requestCount.incrementAndGet();
                        if (!rsp.isFailure()) {
                            requestReceive.incrementAndGet();
                            if (Boolean.parseBoolean(rsp.getHeadValue(CommandConstants.BIT_NOT_EXIST))) {
                                log.warn(String.format(key + " from [%d ~ %d) not exist!!", currentOffsetBegin, currentOffsetEnd));
                            } else {
                                String expireTimeStr = rsp.getHeadValue("expireTime");
                                long expireTime = 0;
                                if (!StringUtils.isEmpty(expireTimeStr)) {
                                    expireTime = Long.parseLong(expireTimeStr);
                                }
                                if (System.currentTimeMillis() < expireTime) {
                                    requestSuccess.incrementAndGet();
                                    log.info(String.format(key + " from [%d ~ %d) exists!!", currentOffsetBegin, currentOffsetEnd));
                                    if (maxByte >= (currentOffsetBegin + rsp.getData().length)) {
                                        System.arraycopy(rsp.getData(), 0, resultByte, currentOffsetBegin, rsp.getData().length);
                                    } else {
                                        System.arraycopy(rsp.getData(), 0, resultByte, currentOffsetBegin, maxByte - currentOffsetBegin);
                                    }
                                } else {
                                    log.warn(String.format(key + " from [%d ~ %d) expired!! expiretime " + expireTime, currentOffsetBegin, currentOffsetEnd));
                                }
                            }
                        } else {
                            requestError.incrementAndGet();
                        }
                    } catch (Exception e) {
                        log.error("get wholeBit value happened error!", e);
                        requestError.incrementAndGet();
                    } finally {
                        if (newKey != null) {
                            connectionControl.offerSession(newKey, session);
                        }
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await(timeOut, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_UNKNOW_ERROR, "waiting was interrupted when getting whole bit!");
        }

        if (latch.getCount() != 0) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_TIMEOUT, "waiting expire when getting whole bit!");
        } else if (requestError.get() > 0) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_READ_FAILURE, "one of multi part error happened!");
        }

        log.info(key + " in getWholeBit request package is " + requestCount.get() + " success count " + requestSuccess.get() + " error count " + requestError.get() + " receive " + requestReceive
                .get());
        return new HippoResult(true, resultByte, 0);
    }

    @Override
    public HippoResult removeWholeBit(final Serializable key, int maxOffset, final int requestExpire, int timeOut) {
        byte[] keybytes = null;
        try {
            keybytes = serializer.serialize(key);
        } catch (IOException e) {
            log.error("serialize bit error when getWholeBit! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        }

        if (maxOffset < 0) {
            maxOffset = 100000001;
        } else {
            maxOffset++;
        }
        final byte[] originalKey = keybytes;

        final int maxByte = (maxOffset % 8 == 0 ? maxOffset / 8 : maxOffset / 8 + 1);
        final AtomicInteger requestCount = new AtomicInteger(0);
        final AtomicInteger requestSuccess = new AtomicInteger(0);
        final AtomicInteger requestError = new AtomicInteger(0);
        final int byteSizeLeft = KeyUtil.getByteSizeLeft(keybytes, CommandConstants.DEFAULT_BIT_BLOCKED_SIZE);

        int bitSizeLeft = byteSizeLeft * 8;
        int allBlockOffset = (maxOffset % bitSizeLeft == 0 ? maxOffset / bitSizeLeft : (maxOffset / bitSizeLeft + 1));
        final byte[] resultByte = new byte[maxByte];

        final CountDownLatch latch = new CountDownLatch(allBlockOffset);
        for (int count = 0; count < allBlockOffset; count++) {
            final int currentOffsetEnd = (count + 1) * byteSizeLeft;
            final byte[] newKey = KeyUtil.getKeyAfterCombineOffset(keybytes, Logarithm.intToBytes(currentOffsetEnd), CommandConstants.DEFAULT_BIT_OP_SEPRATOR);
            service.submit(new Runnable() {
                @Override
                public void run() {
                    Session session = null;
                    try {
                        RemoveCommand command = new RemoveCommand();
                        command.setData(newKey);

                        ClientSessionResult sessionResult = createSession(originalKey);
                        session = sessionResult.getSession();
                        setBucketNumInCommand(command, sessionResult.getBucketNum());

                        Response rsp = (Response) session.send(command, requestExpire * 1000);
                        requestCount.incrementAndGet();

                        if (!rsp.isFailure()) {
                            requestSuccess.incrementAndGet();
                        } else {
                            requestError.incrementAndGet();
                        }
                    } catch (Exception e) {
                        log.error("get wholeBit value happened error!", e);
                        requestError.incrementAndGet();
                    } finally {
                        if (newKey != null) {
                            connectionControl.offerSession(newKey, session);
                        }
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await(timeOut, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_UNKNOW_ERROR, "waiting was interrupted when removing whole bit");
        }

        if (latch.getCount() != 0) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_TIMEOUT, "waiting expire when removing whole bit!");
        } else if (requestError.get() > 0) {
            return new HippoResult(false, HippoCodeDefine.HIPPO_READ_FAILURE, "one of multi part error happened!");
        }

        log.info(key + " in removeWholeBit request package is " + requestCount.get() + " success count " + requestSuccess.get() + " error count " + requestError.get());
        return new HippoResult(true, resultByte, 0);
    }


    @Override
    public HippoResult getBit(Serializable key, int offset, int timeExpire) {
        Session session = null;
        byte[] keybytes = null;
        try {
            GetBitCommand command = new GetBitCommand();
            keybytes = serializer.serialize(key);
            //newKey = getByteAccordingOffset(keybytes, offset);
            command.setData(keybytes);
            command.putHeadValue(CommandConstants.BIT_OFFSET, offset + "");

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());

            Response response = (Response) session.send(command, timeExpire * 1000);
            HippoResult result = null;
            if (response.isFailure()) {
                result = new HippoResult(false, response.getErrorCode(), response.getContent());
            } else {
                if (Boolean.parseBoolean(response.getHeadValue(CommandConstants.BIT_NOT_EXIST))) {
                    result = new HippoResult(true);
                    result.setData(ByteUtil.parseBoolean(false));
                    String extraError = response.getHeadValue(CommandConstants.BIT_GET_EXT_CODE);
                    if (StringUtils.isNotEmpty(extraError)) {
                        result.setErrorCode(extraError);
                    }
                } else {
                    String expireTimeStr = response.getHeadValue("expireTime");
                    long expireTime = 0;
                    if (!StringUtils.isEmpty(expireTimeStr)) {
                        expireTime = Long.parseLong(expireTimeStr);
                    }

                    if (expireTime > 0 && System.currentTimeMillis() > expireTime) {
                        result = new HippoResult(true);
                        result.setData(ByteUtil.parseBoolean(false));
                    } else {
                        result = new HippoResult(true, response.getData(), 0);
                    }
                }
            }
            return result;
        } catch (IOException e) {
            log.error("get bit happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("get bit happened error!", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    @Override
    public HippoResult setBit(Serializable key, int offset, boolean val, int expire, int timeExpire) {
        Session session = null;
        byte[] keybytes = null;
        try {
            SetBitCommand command = new SetBitCommand();
            keybytes = serializer.serialize(key);
            //newKey = getByteAccordingOffset(keybytes, offset);
            command.setExpire(expire);
            command.setData(keybytes);
            command.putHeadValue(CommandConstants.BIT_OFFSET, offset + "");
            command.putHeadValue(CommandConstants.BIT_VAL, val + "");

            ClientSessionResult sessionResult = createSession(keybytes);
            session = sessionResult.getSession();
            setBucketNumInCommand(command, sessionResult.getBucketNum());

            Response response = (Response) session.send(command, timeExpire * 1000);
            HippoResult result = null;
            if (response.isFailure()) {
                result = new HippoResult(false, response.getErrorCode(), response.getContent());
            } else {
                result = new HippoResult(true);
                result.setData(response.getData());
            }
            return result;
        } catch (IOException e) {
            log.error("set bit happened error! ", e);
            return new HippoResult(false, HippoCodeDefine.HIPPO_CLIENT_ERROR, e.getMessage());
        } catch (HippoException e) {
            log.error("set bit happened error!", e);
            return new HippoResult(false, e.getErrorCode(), e.getMessage());
        } finally {
            connectionControl.offerSession(keybytes, session);
        }
    }

    private Command createSetOrUpdateCommand(int expire, byte[] keybytes, byte[] valuebytes, int version) throws HippoException {
        Command command = null;
        byte[] databytes = null;
        databytes = new byte[keybytes.length + valuebytes.length];
        System.arraycopy(keybytes, 0, databytes, 0, keybytes.length);
        System.arraycopy(valuebytes, 0, databytes, keybytes.length, valuebytes.length);
        if (version > 0) {
            command = new UpdateCommand();
            ((UpdateCommand) command).setKlength(keybytes.length);
            ((UpdateCommand) command).setExpire(expire);
        } else {
            command = new SetCommand();
            ((SetCommand) command).setKlength(keybytes.length);
            ((SetCommand) command).setExpire(expire);
        }
        command.setData(databytes);
        return command;
    }

    private void checkDataSize(int length) throws HippoException {
        if (length >= 1024 * 100 && length < 1024 * 1024) {
            log.warn(" data and key length is larger than 100k.  ");
        }
        if (length >= 1024 * 1024) {
            throw new HippoException(" data and key length is larger than 1m.  ");
        }
    }
}
