package org.ncp.metrade.trade;

import com.google.protobuf.Timestamp;
import org.ncp.core.Priority;
import org.ncp.core.RunnableInstance;
import org.ncp.core.Service;
import org.ncp.core.messaging.Publisher;
import org.ncp.core.messaging.utils.MessagingUtils;
import org.ncp.core.util.clock.Clock;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.thread.ThreadFactoryUtils;
import org.ncp.metrade.METrade;
import org.ncp.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.ncp.core.messaging.rabbitmq.RabbitMqPublisher.newPublisher;

@Service(of = {METrade.class})
@Priority(99)
public class OrderRequestGenerator implements RunnableInstance {
    private static final Logger log = LoggerFactory.getLogger(OrderRequestGenerator.class);

    private Publisher<Envelope> publisher;
    private ScheduledExecutorService executorService;
    private int baseSize = 10;
    private double basePrice = 9.5;
    private double lastPrice = 10;
    private final Random random = new Random();
    private String system;
    private String user;

    @Override
    public void init(Context context) throws Exception {
        system = context.getInstance();
        user = context.getUser();
        publisher = newPublisher(context);
        executorService = Executors.newSingleThreadScheduledExecutor(ThreadFactoryUtils.newThreadFactory("orderRequestGenerator"));
    }

    @Override
    public void start() {
        executorService.scheduleAtFixedRate(() -> {
            try {
                publisher.publish(MessagingUtils.packMessage(generateOrderRequest()));
            } catch (Exception e) {
                log.info("Problem publishing order request", e);
                throw new RuntimeException(e);
            }
        }, 1, 5, TimeUnit.SECONDS);
    }

    private OrderRequest generateOrderRequest() {
        return OrderRequest.newBuilder().
                setOrder(generateOrder(
                        Order.Status.NEW,
                        getNextPrice(),
                        getNextSize())).build();
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

    private Order generateOrder(Order.Status status, double price, int size) {
        Timestamp now = Clock.getTimestamp();
        return Order.newBuilder()
                .setStatus(status)
                .setPrice(price)
                .setQuantity(size)
                .setSide(random.nextBoolean() ? Side.BUY : Side.SELL)
                .setTraderId(user)
                .setClientId(system)
                .setClientOrderId(UUID.randomUUID().toString())
                .setAssetId(1)
                .setOrderType(Order.OrderType.MARKET)
                .setCreationTime(now)
                .setLastUpdateTime(now)
                .setVersion(0)
                .build();
    }
}