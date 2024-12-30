package org.ncp.metrade.trade.reference.asset;

import org.ncp.core.Initialisable;
import org.ncp.core.Service;
import org.ncp.core.util.config.Context;
import org.ncp.core.util.datastructure.Cache;
import org.ncp.core.util.datastructure.graph.Reactive;
import org.ncp.model.trade.asset.Asset;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AssetCache extends ConcurrentHashMap<String, Asset> implements Cache<Asset>, Initialisable {

    private final Map<String, Integer> idMap = new HashMap<>();
    private final Map<Integer, String> symbolMap = new HashMap<>();
    private Reactive<Asset> reactive;

    @Override
    public void init(Context context) {
        reactive = context.getGraph().createInputReactive();
    }

    public Collection<Asset> getAll() {
        return values();
    }

    public Asset get(String symbol) {
        return super.get(symbol);
    }

    public Asset get(int id) {
        return has(id) ? super.get(symbolMap.get(id)) : null;
    }

    public boolean has(int id) {
        return symbolMap.containsKey(id);
    }

    public boolean has(String symbol) {
        return get(symbol) != null;
    }

    public String symbolForId(int id) {
        return symbolMap.get(id);
    }

    public int idForSymbol(String symbol) {
        return idMap.get(symbol);
    }

    @Override
    public Asset putIfAbsent(String key, Asset asset) {
        if (!super.containsKey(key)) {
            idMap.put(asset.getSymbol(), asset.getId());
            symbolMap.put(asset.getId(), asset.getSymbol());
            super.put(key, asset);
            if (reactive != null) {
                reactive.evaluate(asset);
            }
            return asset;
        }
        return super.get(key);
    }

    @Override
    public Asset put(String key, Asset asset) {
        idMap.put(asset.getSymbol(), asset.getId());
        symbolMap.put(asset.getId(), asset.getSymbol());
        super.put(key, asset);
        if (reactive != null) {
            reactive.evaluate(asset);
        }
        return asset;
    }

    @Override
    public void putAll(Map<? extends String, ? extends Asset> m) {
        super.putAll(m);
        if (reactive != null) {
            m.values().forEach(v -> reactive.evaluate(v));
        }
    }

    @Override
    public Reactive<Asset> getReactive() {
        return reactive;
    }
}
