package org.ncp.metrade.trade;

import org.ncp.core.service.RunnableInstance;
import org.ncp.core.service.Service;
import org.ncp.core.trade.api.exception.ServiceException;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.SimpleCache;
import org.ncp.core.util.thread.ThreadFactoryUtils;
import org.ncp.metrade.METrade;
import org.ncp.metrade.trade.order.OrderClient;
import org.ncp.metrade.trade.reference.asset.AssetCache;
import org.ncp.metrade.trade.reference.asset.AssetClient;
import org.ncp.model.common.Currency;
import org.ncp.model.trade.asset.Asset;
import org.ncp.model.trade.order.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ncp.model.DataModelUtils.logPrint;

@Service(of = {METrade.class})
public class OrderRequestGenerator implements RunnableInstance {
    private static final Logger log = LoggerFactory.getLogger(OrderRequestGenerator.class);

    private ScheduledExecutorService executorService;
    private int baseSize = 10;
    private double basePrice = 9.5;
    private double lastPrice = 10;
    private final Random random = new Random();
    private String system;
    private AssetClient assetClient;
    private OrderClient orderClient;
    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void init(Context context) throws Exception {
        system = context.getInstance();
        executorService = Executors.newSingleThreadScheduledExecutor(ThreadFactoryUtils.newThreadFactory("orderRequestGenerator"));
        this.orderClient = new OrderClient(context, new SimpleCache<>());
        this.assetClient = new AssetClient(context, context.getInstance(AssetCache.class));
    }

    @Override
    public void start() {
        try {
            Asset asset = assetClient.createAsset("test", "Test Asset", "Test Asset", Currency.GBP);
            log.info("OrderRequestGenerator: created asset: " + logPrint(asset));
            executorService.scheduleAtFixedRate(() -> {
                try {
                    Order order = orderClient.createOrder(
                            asset.getSymbol(),
                            random.nextBoolean() ? Order.OrderType.LIMIT : Order.OrderType.MARKET,
                            random.nextBoolean() ? Order.Side.BUY : Order.Side.SELL,
                            getNextPrice(),
                            getNextSize(),
                            "TESTACC",
                            "TESTORDERID" + counter.getAndIncrement(),
                            system);
                    log.debug("OrderRequestGenerator: created order: {}", logPrint(order));
                } catch (Exception e) {
                    log.info("Problem publishing order request", e);
                    throw new RuntimeException(e);
                }
            }, 1, 50, TimeUnit.MICROSECONDS);
        } catch (ServiceException e) {
            throw new RuntimeException(e);
        }
    }

    private Double getNextPrice() {
        double mV = lastPrice * (random.nextGaussian() * 0.1);
        double mP = (lastPrice + mV);
        double dV = (basePrice - mP) * (Math.abs(random.nextGaussian() * 0.1));
        double v = Math.floor((dV + basePrice) * 100) / 100.0;
        if (Double.compare(v, lastPrice) == 0) {
            v = getNextPrice();
        }
        return v;
    }

    private int getNextSize() {
        double delta = random.nextGaussian()*100;
        return Math.abs(baseSize + (int)delta);
    }
}