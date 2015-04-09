package de.spinscale.elasticsearch.action.suggest.suggest;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ShardSuggestResponse extends BroadcastShardOperationResponse {

    private Map<String,Long> suggestions;

    public ShardSuggestResponse() {}

    public ShardSuggestResponse(String index, int shardId, Map<String,Long> suggestions) {
        super(index, shardId);
        this.suggestions = suggestions;
    }

    public Map<String,Long> getSuggestions() {
        return suggestions;
    }

    public Map<String,Long> suggestions() {
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
}
