package org.ncp.metrade.trade;

import org.ncp.core.Initialisable;
import org.ncp.core.Service;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reaction;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.metrade.METrade;
import org.ncp.metrade.misc.MessageReceiver;
import org.ncp.model.Order;
import org.ncp.model.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.model.DataModelUtils.logPrint;

@Service(of = {METrade.class})
public class OrderRequestProcessor implements Initialisable, Reaction<OrderRequest> {

    private static final Logger log = LoggerFactory.getLogger(OrderRequestProcessor.class);
    private Reactive<Order> orderReactive;

    @Override
    public void init(Context context) throws Exception {
        orderReactive = context.getGraph().createInputReactive();
        context.getGraph().bind(context.getInstance(MessageReceiver.class).getOrderRequestReactive(), this);
    }

    @Override
    public OrderRequest evaluate(OrderRequest request) {
        log.info("OrderRequestProcessor: Got order request: " + logPrint(request));
        orderReactive.evaluate(request.getOrder());
        return request;
    }

    public Reactive<Order> getOrderReactive() {
        return orderReactive;
    }
}
