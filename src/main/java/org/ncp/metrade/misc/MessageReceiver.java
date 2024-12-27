package org.ncp.metrade.misc;

import org.ncp.core.Priority;
import org.ncp.core.RunnableInstance;
import org.ncp.core.Service;
import org.ncp.core.exception.ConsumptionException;
import org.ncp.core.messaging.Queue;
import org.ncp.core.messaging.QueueConsumer;
import org.ncp.core.messaging.rabbitmq.MessageProperties;
import org.ncp.core.messaging.utils.MessagingUtils;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.metrade.METrade;
import org.ncp.model.Envelope;
import org.ncp.model.HeartBeat;
import org.ncp.model.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.core.messaging.rabbitmq.RabbitMqSubscriber.newQueue;
import static org.ncp.core.messaging.utils.MessagingUtils.getMessage;

@Service(of = {METrade.class}, priority = 1)
@Priority(1)
public class MessageReceiver implements RunnableInstance, QueueConsumer<Envelope> {
    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    private Queue<Envelope> listener;
    private Reactive<HeartBeat> heartBeatReactive;
    private Reactive<OrderRequest> orderRequestReactive;

    @Override
    public void init(Context context) throws Exception {
        heartBeatReactive = context.getGraph().createReactive( data -> {
            log.info("MessageReceiver (Heartbeat): " + MessagingUtils.logPrint(data));
            return data;
        });
        orderRequestReactive = context.getGraph().createInputReactive();
        listener = newQueue(context, "listener", this);
    }

    @Override
    public void start() throws Exception {
        listener.start();
    }

    @Override
    public void consume(MessageProperties properties, Envelope message) throws ConsumptionException {
        try {
            switch (message.getKey().getMessageType()) {
                case OrderRequest -> orderRequestReactive.evaluate(getMessage(message));
                case HeartBeat -> heartBeatReactive.evaluate(getMessage(message));
            }
        } catch (Exception e) {
            throw new ConsumptionException(e);
        }
    }

    public Reactive<OrderRequest> getOrderRequestReactive() {
        return orderRequestReactive;
    }
}
