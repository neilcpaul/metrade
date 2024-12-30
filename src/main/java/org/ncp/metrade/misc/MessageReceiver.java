package org.ncp.metrade.misc;

import org.ncp.core.exception.ConsumptionException;
import org.ncp.core.messaging.Queue;
import org.ncp.core.messaging.QueueConsumer;
import org.ncp.core.messaging.rabbitmq.MessageProperties;
import org.ncp.core.messaging.utils.MessagingUtils;
import org.ncp.core.service.Initialisable;
import org.ncp.core.service.Service;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.metrade.METrade;
import org.ncp.model.Envelope;
import org.ncp.model.HeartBeat;
import org.ncp.model.trade.order.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.core.messaging.rabbitmq.RabbitMqSubscriber.newQueue;
import static org.ncp.core.messaging.utils.MessagingUtils.getMessage;
import static org.ncp.model.DataModelUtils.logPrint;

@Service(of = {METrade.class})
public class MessageReceiver implements Initialisable, QueueConsumer<Envelope> {
    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    private Queue<Envelope> listener;
    private Reactive<HeartBeat> heartBeatReactive;
    private Reactive<OrderRequest> orderRequestReactive;

    @Override
    public void init(Context context) throws Exception {
        heartBeatReactive = context.getGraph().createReactive( data -> {
            log.info("MessageReceiver (Heartbeat): {}", MessagingUtils.logPrint(data));
            return data;
        });
        orderRequestReactive = context.getGraph().createReactive( data -> {
            log.info("MessageReceiver (OrderRequest): {}", MessagingUtils.logPrint(data));
            return data;
        });
        newQueue(context, "all-messages", this).startAsync();
    }

    @Override
    public void consume(MessageProperties properties, Envelope message) throws ConsumptionException {
        try {
            switch (message.getKey().getMessageType()) {
                case OrderRequest -> orderRequestReactive.evaluate(getMessage(message));
                case HeartBeat -> heartBeatReactive.evaluate(getMessage(message));
                default -> printMessage(message);
            }
        } catch (Exception e) {
            throw new ConsumptionException(e);
        }
    }

    private void printMessage(Envelope message) {
        log.info("MessageReceiver: saw message: {}", logPrint(message));
    }

    public Reactive<OrderRequest> getOrderReactive() {
        return orderRequestReactive;
    }
}
