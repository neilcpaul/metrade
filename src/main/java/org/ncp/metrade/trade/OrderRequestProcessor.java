package org.ncp.metrade.trade;

import org.ncp.core.Initialisable;
import org.ncp.core.Service;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reaction;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.core.util.datastructure.graph.ReactiveProvider;
import org.ncp.metrade.METrade;
import org.ncp.metrade.misc.MessageReceiver;
import org.ncp.metrade.trade.reference.asset.AssetCache;
import org.ncp.model.trade.order.Order;
import org.ncp.model.trade.order.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.model.DataModelUtils.logPrint;

public class OrderRequestProcessor implements Initialisable, Reaction<OrderRequest>, ReactiveProvider<Order> {

    private static final Logger log = LoggerFactory.getLogger(OrderRequestProcessor.class);
    private Reactive<Order> orderReactive;
    private AssetCache assetCache;

    @Override
    public void init(Context context) throws Exception {
        orderReactive = context.getGraph().createInputReactive();
        context.getGraph().bind(context.getInstance(MessageReceiver.class).getOrderReactive(), this);
        this.assetCache = context.getInstance(AssetCache.class);
    }

    @Override
    public OrderRequest evaluate(OrderRequest request) {
        if (isValid(request)) {
            orderReactive.evaluate(request.getOrder());
            return request;
        } else {
            log.warn("OrderRequestProcessor: Invalid order request: " + logPrint(request));
        }
        return null;
    }

    @Override
    public Reactive<Order> getReactive() {
        return orderReactive;
    }

    public boolean isValid(OrderRequest request) {
        return assetCache.has(request.getOrder().getSymbol());
    }
}
