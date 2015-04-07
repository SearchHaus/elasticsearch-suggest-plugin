package de.spinscale.elasticsearch.action.suggest.suggest;

import de.spinscale.elasticsearch.service.suggest.ShardSuggestService;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableSortedSet;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class TransportSuggestAction extends TransportBroadcastOperationAction<SuggestRequest, SuggestResponse, ShardSuggestRequest, ShardSuggestResponse> {

	private static final boolean sortAlpha = true;
	
    private final IndicesService indicesService;

    @Inject public TransportSuggestAction(Settings settings, ThreadPool threadPool,
            ClusterService clusterService, TransportService transportService,
            IndicesService indicesService) {
        super(settings, SuggestAction.NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override
    protected SuggestRequest newRequest() {
        return new SuggestRequest();
    }

    @Override
    protected SuggestResponse newResponse(SuggestRequest request,
            AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        logger.trace("Entered TransportSuggestAction.newResponse()");

        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        Map<String,Long> items = new HashMap<String,Long>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else if (shardResponse instanceof ShardSuggestResponse) {
                ShardSuggestResponse shardSuggestResponse = (ShardSuggestResponse) shardResponse;
                Map<String,Long> shardItems = shardSuggestResponse.suggestions();
                // add all shard items to items, sum up weights
                Long prevValue;
                for (Map.Entry<String,Long> entry : shardItems.entrySet())
                	if ((prevValue = items.get(entry.getKey())) == null)	// new entry
                		items.put(entry.getKey(), entry.getValue());
                	else if (prevValue + entry.getValue() >= prevValue)	// no overflow (assuming non negative weights)
                		items.put(entry.getKey(), entry.getValue() + prevValue);
                	else	// overflow
                		items.put(entry.getKey(), Long.MAX_VALUE);

                successfulShards++;
            } else {
                successfulShards++;
            }
        }

        // sort items
        Map<String,Long> resultItems = new LinkedHashMap<String,Long>();
        LinkedList<Map.Entry<String,Long>> list = new LinkedList<Map.Entry<String,Long>>(items.entrySet());
        if (sortAlpha) {
    		Collections.sort(list, new Comparator<Map.Entry<String,Long>>() {
    			@Override
    			public int compare(Map.Entry<String,Long> o1, Map.Entry<String,Long> o2) {
    				return o1.getKey().compareTo(o2.getKey());
    			}
    		});
        }
        else {
    		Collections.sort(list, new Comparator<Map.Entry<String,Long>>() {
    			@Override
    			public int compare(Map.Entry<String,Long> o1, Map.Entry<String,Long> o2) {
    				return o1.getValue().compareTo(o2.getValue());
    			}
    		});
        }
		for (Map.Entry<String,Long> entry : list.subList(0, Math.min(list.size(), request.size())))	// sublist
			resultItems.put(entry.getKey(), entry.getValue());
        
        return new SuggestResponse(resultItems, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardSuggestRequest newShardRequest() {
        return new ShardSuggestRequest();
    }

    @Override
    protected ShardSuggestRequest newShardRequest(int numShards, ShardRouting shard, SuggestRequest request) {
        return new ShardSuggestRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardSuggestResponse newShardResponse() {
        return new ShardSuggestResponse();
    }

    @Override
    protected ShardSuggestResponse shardOperation(ShardSuggestRequest request) throws ElasticsearchException {
        logger.trace("Entered TransportSuggestAction.shardOperation()");
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        ShardSuggestService suggestShardService = indexService.shardInjectorSafe(request.shardId()).getInstance(ShardSuggestService.class);
        return suggestShardService.suggest(request);
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState,
            SuggestRequest request, String[] concreteIndices) {
        logger.trace("Entered TransportSuggestAction.shards()");
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, null, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SuggestRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, SuggestRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

}
