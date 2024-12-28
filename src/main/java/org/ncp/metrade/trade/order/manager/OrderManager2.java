package org.ncp.metrade.trade.order.manager;

import org.ncp.core.Initialisable;
import org.ncp.core.Service;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reaction;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.metrade.METrade;
import org.ncp.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.model.DataModelUtils.logPrint;

@Service(of = {METrade.class})
public class OrderManager2 implements Initialisable, Reaction<Order>{

    private static final Logger log = LoggerFactory.getLogger(OrderManager2.class);
    Reactive<Boolean> started;

    @Override
    public void init(Context context) throws Exception {
        log.info("Started OrderManager2");
        started = context.getGraph().createInputReactive();
        context.getGraph().bind(context.getInstance(OrderManager.class).getStartedReactive(), r -> r);
    }

    @Override
    public Order evaluate(Order data) {
        log.info("OrderManager: Got order: " + logPrint(data));
        return data;
    }

    public Reactive<Boolean> getStartedReactive() {
        return started;
    }
}
