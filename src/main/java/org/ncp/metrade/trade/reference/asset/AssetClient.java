package org.ncp.metrade.trade.reference.asset;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.ncp.core.exception.ConsumptionException;
import org.ncp.core.exception.MessagingException;
import org.ncp.core.messaging.rabbitmq.MessageProperties;
import org.ncp.core.messaging.utils.MessagingUtils;
import org.ncp.core.trade.api.exception.*;
import org.ncp.core.trade.api.impl.AbstractRpcApi;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.core.util.datastructure.graph.ReactiveProvider;
import org.ncp.model.*;
import org.ncp.model.Currency;
import org.ncp.model.Error;
import org.ncp.model.trade.asset.Asset;
import org.ncp.model.trade.asset.AssetCreationRequest;
import org.ncp.model.trade.asset.AssetList;
import org.ncp.model.trade.asset.AssetListRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.ncp.core.messaging.utils.MessagingUtils.getMessage;
import static org.ncp.model.DataModelUtils.logPrint;

public class AssetClient extends AbstractRpcApi<Asset> implements ReactiveProvider<Asset> {

    private final static Logger log = LoggerFactory.getLogger(AssetClient.class);
    private Reactive<Asset> reactive;

    public AssetClient(Context context, AssetCache assetCache) throws MessagingException {
        super(context, "assetClient", assetCache);
        reactive = context.getGraph().createInputReactive();
    }

    @Override
    protected Collection<Asset> prefetch(boolean fullPrefetch, String authId) throws DataPrefetchException {
        try {
            AssetList assets = sendRequest(Key.MessageType.AssetList, AssetListRequest.getDefaultInstance());
            return assets.getAssetsList();
        } catch (InvalidRpcResponseException e) {
            throw new DataPrefetchException(e);
        }
    }

    @Override
    protected String prefetchKey(Asset data) {
        return data.getSymbol();
    }

    @Override
    public void consume(MessageProperties properties, Envelope message) throws ConsumptionException {
        try {
            Asset asset = getMessage(message);
            getCache().putIfAbsent(prefetchKey(asset), asset);
            reactive.evaluate(asset);
        } catch (InvalidProtocolBufferException e) {
            throw new ConsumptionException(e);
        }
    }

    @Override
    public Collection<Key.MessageType> getMessageTypes() {
        return Set.of(Key.MessageType.Asset);
    }

    public Asset createAsset(String symbol, String assetName, String assetDescription, org.ncp.model.Currency currency) throws ServiceException {
        if (getCache().has(symbol)) {
            log.warn("AssetClient: asset {} already exists", symbol);
            return getCache().get(symbol);
        }
        try {
            log.info("AssetClient: Sending asset creation request of asset symbol {}", symbol);
            return sendRequest(Key.MessageType.Asset, createAssetCreationRequest(symbol, assetName, assetDescription, currency));
        } catch (InvalidRpcResponseException e) {
            throw new AssetCreationException("Could not create asset: " + e.getMessage(), e);
        }
    }

    private AssetCreationRequest createAssetCreationRequest(
            String symbol,
            String assetName,
            String description,
            Currency currency
    ) {
        return AssetCreationRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setSymbol(symbol)
                        .setName(assetName)
                        .setDescription(description)
                        .setCurrency(currency)
                        .build())
                .build();
    }

    @Override
    public Reactive<Asset> getReactive() {
        return reactive;
    }
}
