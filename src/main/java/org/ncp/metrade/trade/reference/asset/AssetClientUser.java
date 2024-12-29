package org.ncp.metrade.trade.reference.asset;

import org.ncp.core.RunnableInstance;
import org.ncp.core.Service;
import org.ncp.core.util.config.Context;
import org.ncp.model.Currency;
import org.ncp.model.trade.asset.Asset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.model.DataModelUtils.logPrint;

@Service
public class AssetClientUser implements RunnableInstance {

    private final static Logger log = LoggerFactory.getLogger(AssetClientUser.class);
    private AssetClient assetClient;

    @Override
    public void init(Context context) throws Exception {
        AssetCache cache = context.getInstance(AssetCache.class);
        this.assetClient = new AssetClient(context, cache);
        context.getGraph().bind(assetClient.getReactive(), this::processAsset);
    }

    private Asset processAsset(Asset asset) {
        log.info("AssetServiceUser: Got asset from reactive: {}", logPrint(asset));
        return asset;
    }

    @Override
    public void start() throws Exception {
        Asset asset = assetClient.createAsset("test", "Test Asset", "Test Asset", Currency.GBP);
        log.info("AssetClientUser: got asset: {}", logPrint(asset));
    }
}
