package de.spinscale.elasticsearch.action.suggest.suggest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

public class SuggestResponse extends BroadcastOperationResponse implements ToXContent {

    private Map<String,Long> suggestions;

    public SuggestResponse() {
    }

    public SuggestResponse(Map<String,Long> suggestions, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.suggestions = suggestions;
    }

    public List<String> suggestions() {
        return Lists.newArrayList(suggestions.keySet());
    }

    public List<String>  getSuggestions() {
        return Lists.newArrayList(suggestions.keySet());
    }
    
    public Map<String,Long>  getWeightedSuggestions() {
        return suggestions;
    }
    
    public Map<String,Long>  weightedSuggestions() {
        return suggestions;
    }
    
    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        suggestions = (Map<String,Long>) in.readGenericValue();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericValue(suggestions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        buildBroadcastShardsHeader(builder, this);
        builder.field("suggestions", suggestions);
        return builder;
    }
}
