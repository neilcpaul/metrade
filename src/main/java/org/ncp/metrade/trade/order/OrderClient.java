package org.ncp.metrade.trade.order;

import com.google.protobuf.InvalidProtocolBufferException;
import org.ncp.core.exception.ConsumptionException;
import org.ncp.core.exception.MessagingException;
import org.ncp.core.messaging.rabbitmq.ActiveSession;
import org.ncp.core.messaging.rabbitmq.MessageProperties;
import org.ncp.core.trade.api.exception.*;
import org.ncp.core.trade.api.impl.AbstractRpcApi;
import org.ncp.core.util.clock.Clock;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.Cache;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.core.util.datastructure.graph.ReactiveProvider;
import org.ncp.metrade.trade.reference.asset.AssetCache;
import org.ncp.model.Envelope;
import org.ncp.model.Key;
import org.ncp.model.common.Currency;
import org.ncp.model.trade.asset.Asset;
import org.ncp.model.trade.asset.AssetCreationRequest;
import org.ncp.model.trade.asset.AssetList;
import org.ncp.model.trade.asset.AssetListRequest;
import org.ncp.model.trade.order.Order;
import org.ncp.model.trade.order.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.ncp.core.messaging.utils.MessagingUtils.getMessage;
import static org.ncp.model.DataModelUtils.logPrint;

public class OrderClient extends AbstractRpcApi<Order> implements ReactiveProvider<Order> {

    private final static Logger log = LoggerFactory.getLogger(OrderClient.class);
    private final Reactive<Order> reactive;
    private final AssetCache assetCache;
    private final String user;

    public OrderClient(Context context, Cache<Order> cache) throws Exception {
        super(context, "orderClient", cache);
        reactive = context.getGraph().createInputReactive();
        this.assetCache = context.getInstance(AssetCache.class);
        user = context.getUser();
    }

    @Override
    protected Collection<Order> prefetch(boolean fullPrefetch, String authId) throws DataPrefetchException {
        // TODO: neil - implement
        return List.of();
    }

    @Override
    protected String prefetchKey(Order data) {
        return data.getClientId();
    }

    @Override
    public void consume(MessageProperties properties, Envelope message) throws ConsumptionException {
        try {
            Order order = getMessage(message);
            getCache().putIfAbsent(prefetchKey(order), order);
            reactive.evaluate(order);
            log.info("OrderClient: cached order update: {}", logPrint(order));
        } catch (InvalidProtocolBufferException e) {
            throw new ConsumptionException(e);
        }
    }

    @Override
    public Collection<Key.MessageType> getMessageTypes() {
        return Set.of(Key.MessageType.Asset);
    }

    public Order createOrder(String symbol, Order.OrderType orderType, Order.Side side, double price, int quantity, String account, String orderId, String clientId) throws OrderEntryException {
        if (!assetCache.has(symbol)) {
            log.warn("OrderClient: asset {} doesn't exist", symbol);
            throw new OrderEntryException("Order symbol does not exist");
        }
        try {
            log.info("OrderClient: Sending order creation request for symbol{}", symbol);
            return sendRequest(Key.MessageType.Order, createOrderRequestRequest(symbol, orderType, side, price, quantity, account, orderId, user, clientId));
        } catch (InvalidRpcResponseException e) {
            throw new OrderEntryException("Could not create order: " + e.getMessage(), e);
        }
    }

    private OrderRequest createOrderRequestRequest(
            String symbol, Order.OrderType orderType, Order.Side side, double price, int quantity, String accountId, String clientOrderId, String user, String clientId
    ) {
        Order.Builder orderBuilder = Order.newBuilder();
        orderBuilder.setStatus(Order.Status.NEW);
        orderBuilder.setAccountId(accountId);
        orderBuilder.setSymbol(symbol);
        orderBuilder.setPrice(price);
        orderBuilder.setQuantity(quantity);
        orderBuilder.setClientOrderId(clientOrderId);
        orderBuilder.setOrderType(orderType);
        orderBuilder.setSide(side);
        orderBuilder.setTraderId(user);
        orderBuilder.setClientId(clientId);
        orderBuilder.setCreationTime(Clock.getTimestamp());

        return OrderRequest.newBuilder().setOrder(orderBuilder).build();
    }

    @Override
    public Reactive<Order> getReactive() {
        return reactive;
    }
}
