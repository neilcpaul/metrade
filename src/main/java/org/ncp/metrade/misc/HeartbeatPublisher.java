package org.ncp.metrade.misc;

import org.ncp.core.RunnableInstance;
import org.ncp.core.Service;
import org.ncp.core.exception.PublishException;
import org.ncp.core.messaging.Publisher;
import org.ncp.core.messaging.utils.MessagingUtils;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.thread.ThreadFactoryUtils;
import org.ncp.metrade.METrade;
import org.ncp.model.Envelope;
import org.ncp.model.HeartBeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.ncp.core.messaging.rabbitmq.RabbitMqPublisher.newPublisher;

@Service(of = {METrade.class})
public class HeartbeatPublisher implements RunnableInstance {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatPublisher.class);

    private Publisher<Envelope> publisher;
    private ScheduledExecutorService executorService;

    @Override
    public void init(Context context) throws Exception {
        publisher = newPublisher(context, "heartbeat");
        executorService = Executors.newSingleThreadScheduledExecutor(ThreadFactoryUtils.newThreadFactory("heartbeat"));
    }

    @Override
    public void start() {
        executorService.scheduleAtFixedRate(() -> {
            try {
                publisher.publish(MessagingUtils.packMessage(generateHeartbeat()));
            } catch (PublishException e) {
                throw new RuntimeException(e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private HeartBeat generateHeartbeat() {
        return HeartBeat.newBuilder().build();
    }
}
