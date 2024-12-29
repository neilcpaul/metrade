package org.ncp.metrade.trade.reference.asset;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.ncp.core.Initialisable;
import org.ncp.core.Service;
import org.ncp.core.exception.PublishException;
import org.ncp.core.messaging.Publisher;
import org.ncp.core.messaging.RpcProcessor;
import org.ncp.core.messaging.rabbitmq.MessageProperties;
import org.ncp.core.util.clock.Clock;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.core.util.datastructure.graph.ReactiveProvider;
import org.ncp.model.Envelope;
import org.ncp.model.Key;
import org.ncp.model.trade.asset.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ncp.core.messaging.rabbitmq.RabbitMqPublisher.newPublisher;
import static org.ncp.core.messaging.utils.MessagingUtils.getMessage;
import static org.ncp.core.messaging.utils.MessagingUtils.packMessage;

@Service()
public class AssetService implements RpcProcessor<Envelope>, Initialisable, ReactiveProvider<Asset> {

    private final static Logger log = LoggerFactory.getLogger(AssetService.class);
    private final AtomicInteger assetIdCounter = new AtomicInteger(0);
    private Publisher<Envelope> broadcast;
    private Reactive<Asset> assetReactive;
    private AssetCache assetCache;
    private Context context;

    @Override
    public void init(Context context) throws Exception {
        this.context = context;
        this.broadcast = newPublisher(context);
        this.assetCache = context.getInstance(AssetCache.class);
        this.assetReactive = context.getGraph().createInputReactive();
    }

    @Override
    public Collection<Key.MessageType> getMessageTypes() {
        return List.of(Key.MessageType.AssetCreationRequest, Key.MessageType.AssetRequest, Key.MessageType.AssetListRequest);
    }

    @Override
    public Envelope process(MessageProperties properties, Envelope data) {
        Envelope envelope = null;

        try {
            Message response;
            switch (data.getKey().getMessageType()) {
                case AssetCreationRequest -> response = handleAssetCreationRequest(data);
                case AssetRequest -> response = handleAssetRequest(getMessage(data));
                case AssetListRequest -> response = handleAssetListRequest(getMessage(data));
                default -> response = handleError(getMessage(data), "Invalid request");
            }
            envelope = packMessage(response);

            if (data.getKey().getMessageType() == Key.MessageType.AssetCreationRequest
                    && response instanceof Asset) {
                broadcast.publish(envelope);
            }
        } catch (InvalidProtocolBufferException e) {
            log.warn("AssetService: Got bad request: {}", e.getMessage());
            envelope = packMessage(org.ncp.model.Error.newBuilder()
                    .setMessageType(data.getKey().getMessageType())
                    .setMessage("Bad request").build());
        } catch (PublishException e) {
            log.error("AssetService: Could not publish message: {}", e.getMessage());
        }

        return envelope;
    }

    private Message handleAssetCreationRequest(Envelope data) {
        Asset asset = null;
        try {
            AssetCreationRequest request = getMessage(data);

            if (request.hasAsset() && request.getAsset().hasSymbol()
                    && request.getAsset().hasName()) {
                if (!assetCache.has(request.getAsset().getSymbol())) {
                    asset = request.getAsset().toBuilder()
                            .setCreateUser(context.getUser())
                            .setId(assetIdCounter.incrementAndGet())
                            .setCreateTime(Clock.getTimestamp()).build();
                } else {
                    asset = assetCache.get(request.getAsset().getSymbol());
                }
            }
        } catch (InvalidProtocolBufferException e) {
            log.warn("AssetService: Could not process AssetCreationRequest");
        }

        if (asset != null) {
            assetReactive.evaluate(asset);
            return asset;
        } else {
            return handleError(data, "Invalid creation request");
        }
    }

    private AssetList handleAssetListRequest(AssetListRequest request) {
        AssetList.Builder list = AssetList.newBuilder();
        list.addAllAssets(assetCache.getAll());
        return list.build();
    }

    private AssetList handleAssetRequest(AssetRequest request) {
        AssetList.Builder list = AssetList.newBuilder();
        Set<String> failures = new HashSet<>();
        request.getAssetIdList().forEach(req -> {
            Asset a = assetCache.get(req);
            if (a != null) {
                list.addAssets(a);
            } else {
                failures.add(req);
            }
        });
        if (!failures.isEmpty()) {
            log.warn("AssetService: Got request for unknown asset(s): {}", failures);
        }
        return list.build();
    }

    private Message handleError(Envelope data, String message) {
        log.warn("AssetService: {}: {}", message, data.getKey().getMessageType().name());
        return org.ncp.model.Error.newBuilder().setMessageType(data.getKey().getMessageType()).setMessage(message).build();
    }

    @Override
    public Reactive<Asset> getReactive() {
        return assetReactive;
    }
}
