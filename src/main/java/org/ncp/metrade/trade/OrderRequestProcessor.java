package org.ncp.metrade.trade;

import org.ncp.core.Priority;
import org.ncp.core.RunnableInstance;
import org.ncp.core.Service;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reaction;
import org.ncp.metrade.METrade;
import org.ncp.metrade.misc.MessageReceiver;
import org.ncp.model.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.model.DataModelUtils.logPrint;

@Service(of = {METrade.class})
@Priority(50)
public class OrderRequestProcessor implements RunnableInstance, Reaction<OrderRequest> {

    private static final Logger log = LoggerFactory.getLogger(OrderRequestProcessor.class);

    @Override
    public void init(Context context) throws Exception {
        context.getGraph().bind(context.getInstance(MessageReceiver.class).getOrderRequestReactive(), this);
    }

    @Override
    public void start() throws Exception {
        // do nothing
    }

    @Override
    public OrderRequest evaluate(OrderRequest request) {
        log.info("OrderRequestProcessor: Got order request: {}", logPrint(request));
        return request;
    }
}
