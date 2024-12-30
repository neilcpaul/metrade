package org.ncp.metrade.trade.order.manager;

import org.ncp.core.exception.ConsumptionException;
import org.ncp.core.service.Initialisable;
import org.ncp.core.service.Service;
import org.ncp.core.trade.book.AssetMatchingEngine;
import org.ncp.core.trade.book.AssetMatchingEngineImpl;
import org.ncp.core.trade.book.event.OrderEvent;
import org.ncp.core.trade.book.event.PositionOpenEvent;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reaction;
import org.ncp.metrade.METrade;
import org.ncp.metrade.trade.order.OrderService;
import org.ncp.metrade.trade.reference.asset.AssetCache;
import org.ncp.metrade.trade.reference.asset.AssetService;
import org.ncp.model.trade.asset.Asset;
import org.ncp.model.trade.order.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.ncp.model.DataModelUtils.logPrint;

@Service(of = {METrade.class})
public class OrderManager implements Initialisable, Reaction<Order> {

    private static final Logger log = LoggerFactory.getLogger(OrderManager.class);
    private final Map<String, AssetMatchingEngine> matchingEngines = new HashMap<>();
    private AssetCache assetCache;

    @Override
    public void init(Context context) throws Exception {
        log.info("Started OrderManager");
        this.assetCache = context.getInstance(AssetCache.class);
        context.getGraph().bind(context.getInstance(AssetService.class).getReactive(), this::processAssetUpdate);
        context.getGraph().bind(context.getInstance(OrderService.class).getReactive(), this);
    }

    private Asset processAssetUpdate(Asset asset) {
        AssetMatchingEngine ame = new AssetMatchingEngineImpl(asset, this::consumeAmeEvent);
        matchingEngines.put(asset.getSymbol(), ame);
        return asset;
    }

    private void consumeAmeEvent(OrderEvent orderEvent) {
        log.info("OrderManager: received AME event: {}", orderEvent);
    }

    @Override
    public Order evaluate(Order data) {
        log.info("OrderManager: Got order: {}", logPrint(data));
        try {
            processOrder(data);
        } catch (ConsumptionException e) {
            log.error("Could not consume order: " + e.getMessage(), e);
            return null;
        }
        return data;
    }

    private void processOrder(Order order) throws ConsumptionException {
        AssetMatchingEngine engine = matchingEngines.get(order.getSymbol());
        engine.consume(createOrderEvent(order));
    }

    private OrderEvent createOrderEvent(Order order) {
        return new PositionOpenEvent(
                order.getOrderId(),
                order.getSide(),
                assetCache.idForSymbol(order.getSymbol()),
                order.getQuantity(),
                order.getOrderType(),
                order.getPrice()
        );
    }
}
