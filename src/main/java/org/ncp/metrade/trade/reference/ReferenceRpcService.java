package org.ncp.metrade.trade.reference;

import org.ncp.core.Initialisable;
import org.ncp.core.Service;
import org.ncp.core.messaging.rabbitmq.RabbitMqRpcService;
import org.ncp.core.util.config.Context;
import org.ncp.metrade.trade.reference.asset.AssetService;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

@Service(priority = 10)
public class ReferenceRpcService implements Initialisable {

    private final static Logger log = getLogger(ReferenceRpcService.class);

    @Override
    public void init(Context context) throws Exception {
        log.info("ReferenceRpcService: Starting reference service");

        AssetService service = context.getInstance(AssetService.class);

        RabbitMqRpcService rpcService = new RabbitMqRpcService(
                context,
                "assetService",
                service);

        rpcService.startAsync();
    }
}
