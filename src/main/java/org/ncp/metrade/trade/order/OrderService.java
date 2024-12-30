package org.ncp.metrade.trade.order;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.ncp.core.Initialisable;
import org.ncp.core.RunnableInstance;
import org.ncp.core.Service;
import org.ncp.core.exception.MethodNotImplementedException;
import org.ncp.core.messaging.RpcProcessor;
import org.ncp.core.messaging.rabbitmq.MessageProperties;
import org.ncp.core.messaging.rabbitmq.RabbitMqRpcService;
import org.ncp.core.util.clock.Clock;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.crypto.IdentifierUtils;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.core.util.datastructure.graph.ReactiveProvider;
import org.ncp.metrade.trade.reference.asset.AssetCache;
import org.ncp.model.Envelope;
import org.ncp.model.Key;
import org.ncp.model.trade.order.Order;
import org.ncp.model.trade.order.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static org.ncp.core.messaging.utils.MessagingUtils.getMessage;
import static org.ncp.core.messaging.utils.MessagingUtils.packMessage;

@Service(priority = 10)
public class OrderService implements RpcProcessor<Envelope>, Initialisable, ReactiveProvider<Order> {

    private final static Logger log = LoggerFactory.getLogger(OrderService.class);
    private Reactive<Order> reactive;
    private AssetCache assetCache;
    private Context context;

    @Override
    public void init(Context context) throws Exception {
        this.context = context;
        reactive = context.getGraph().createInputReactive();
        this.assetCache = context.getInstance(AssetCache.class);
        listen();
    }

    public void listen() throws Exception {
        RabbitMqRpcService rpcService = new RabbitMqRpcService(
                context,
                "orderService",
                this);

        rpcService.startAsync();
    }

    @Override
    public Collection<Key.MessageType> getMessageTypes() {
        return List.of(Key.MessageType.OrderRequest, Key.MessageType.OrderUpdate);
    }

    @Override
    public Envelope process(MessageProperties properties, Envelope data) {
        Envelope envelope;

        try {
            Message response;
            switch (data.getKey().getMessageType()) {
                case OrderRequest -> response = handleOrderRequest(data);
                case OrderUpdate -> response = handleOrderUpdate(getMessage(data));
                default -> response = handleError(getMessage(data), "Invalid request");
            }
            envelope = packMessage(response);
        } catch (InvalidProtocolBufferException e) {
            log.warn("OrderService: Got bad request: {}", e.getMessage());
            envelope = packMessage(org.ncp.model.Error.newBuilder()
                    .setMessageType(data.getKey().getMessageType())
                    .setMessage("Bad request").build());
        }

        return envelope;
    }

    private Message handleOrderUpdate(Message message) {
        // TODO: neil - implement
        throw new MethodNotImplementedException("OrderService: handleOrderUpdate not implemented");
    }

    private Message handleOrderRequest(Envelope data) {
        Order order = null;
        try {
            OrderRequest request = getMessage(data);

            if (request.hasOrder() && isValid(request)) {
                order = request.getOrder().toBuilder()
                        .setOrderId(IdentifierUtils.getInstance().nextId())
                        .setStatus(Order.Status.ACCEPTED)
                        .setUpdateTime(Clock.getTimestamp()).build();
            }
        } catch (InvalidProtocolBufferException e) {
            log.warn("AssetService: Could not process OrderRequest");
            e.printStackTrace();
        }

        if (order != null) {
            reactive.evaluate(order);
            return order;
        } else {
            return handleError(data, "Invalid creation request");
        }
    }

    private Message handleError(Envelope data, String message) {
        log.warn("AssetService: {}: {}", message, data.getKey().getMessageType().name());
        return org.ncp.model.Error.newBuilder().setMessageType(data.getKey().getMessageType()).setMessage(message).build();
    }

    @Override
    public Reactive<Order> getReactive() {
        return reactive;
    }

    public boolean isValid(OrderRequest request) {
        return assetCache.has(request.getOrder().getSymbol());
    }
}
