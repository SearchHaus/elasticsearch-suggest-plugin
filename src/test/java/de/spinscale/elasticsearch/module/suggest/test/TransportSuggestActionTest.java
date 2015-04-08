package de.spinscale.elasticsearch.module.suggest.test;

import de.spinscale.elasticsearch.action.suggest.refresh.SuggestRefreshAction;
import de.spinscale.elasticsearch.action.suggest.refresh.SuggestRefreshRequest;
import de.spinscale.elasticsearch.action.suggest.statistics.FstStats;
import de.spinscale.elasticsearch.action.suggest.statistics.SuggestStatisticsAction;
import de.spinscale.elasticsearch.action.suggest.statistics.SuggestStatisticsRequest;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestAction;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestRequest;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestResponse;

import org.elasticsearch.common.Strings;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.SUITE, transportClientRatio = 0.0)
public class TransportSuggestActionTest extends AbstractSuggestTest {

    @Override
    public Map<String,Long> getWeightedSuggestions(SuggestionQuery suggestionQuery) throws Exception {
        SuggestRequest request = new SuggestRequest(suggestionQuery.index);

        request.term(suggestionQuery.term);
        request.field(suggestionQuery.field);
        request.sortByFrequency(suggestionQuery.sortByFrequency);

        if (suggestionQuery.size != null) {
            request.size(suggestionQuery.size);
        }
        if (suggestionQuery.similarity != null && suggestionQuery.similarity > 0.0 && suggestionQuery.similarity < 1.0) {
            request.similarity(suggestionQuery.similarity);
        }
        if (suggestionQuery.suggestType != null) {
            request.suggestType(suggestionQuery.suggestType);
        }
        if (Strings.hasLength(suggestionQuery.indexAnalyzer)) {
            request.indexAnalyzer(suggestionQuery.indexAnalyzer);
        }
        if (Strings.hasLength(suggestionQuery.queryAnalyzer)) {
            request.queryAnalyzer(suggestionQuery.queryAnalyzer);
        }
        if (Strings.hasLength(suggestionQuery.analyzer)) {
            request.analyzer(suggestionQuery.analyzer);
        }

        request.preservePositionIncrements(suggestionQuery.preservePositionIncrements);

        SuggestResponse suggestResponse = client().execute(SuggestAction.INSTANCE, request).actionGet();
        assertThat(suggestResponse.getShardFailures(), is(emptyArray()));

        return suggestResponse.weightedSuggestions();
    }

    @Override
    public void refreshAllSuggesters() throws Exception {
        SuggestRefreshRequest refreshRequest = new SuggestRefreshRequest();
        client().execute(SuggestRefreshAction.INSTANCE, refreshRequest).actionGet();
    }

    @Override
    public void refreshIndexSuggesters(String index) throws Exception {
        SuggestRefreshRequest refreshRequest = new SuggestRefreshRequest(index);
        client().execute(SuggestRefreshAction.INSTANCE, refreshRequest).actionGet();
    }

    @Override
    public void refreshFieldSuggesters(String index, String field) throws Exception {
        SuggestRefreshRequest refreshRequest = new SuggestRefreshRequest(index);
        refreshRequest.field(field);
        client().execute(SuggestRefreshAction.INSTANCE, refreshRequest).actionGet();
    }

    @Override
    public FstStats getStatistics() throws Exception {
        SuggestStatisticsRequest suggestStatisticsRequest = new SuggestStatisticsRequest();
        return client().execute(SuggestStatisticsAction.INSTANCE, suggestStatisticsRequest).actionGet().fstStats();
    }

}
