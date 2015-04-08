package de.spinscale.elasticsearch.module.suggest.test;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import de.spinscale.elasticsearch.action.suggest.refresh.SuggestRefreshAction;
import de.spinscale.elasticsearch.action.suggest.refresh.SuggestRefreshRequest;
import de.spinscale.elasticsearch.action.suggest.statistics.FstStats;
import de.spinscale.elasticsearch.action.suggest.statistics.SuggestStatisticsAction;
import de.spinscale.elasticsearch.action.suggest.statistics.SuggestStatisticsRequest;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestAction;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestRequest;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestResponse;

@ClusterScope(scope = Scope.SUITE, transportClientRatio = 0.0)
public class SortOrderTest extends AbstractSuggestTest {

    @Override
    public List<String> getSuggestions(SuggestionQuery suggestionQuery) throws Exception {
        List<String> list = new LinkedList<String>(getWeightedSuggestions(suggestionQuery).keySet());
        return list;
    }
    
    private Map<String,Long> getWeightedSuggestions(SuggestionQuery suggestionQuery) {
        SuggestRequest request = new SuggestRequest(suggestionQuery.index);

        request.term(suggestionQuery.term);
        request.field(suggestionQuery.field);

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
        request.sortByFrequency(suggestionQuery.sortByFrequency);
        
        SuggestResponse suggestResponse = client().execute(SuggestAction.INSTANCE, request).actionGet();
        assertThat(suggestResponse.getShardFailures(), is(emptyArray()));

        return suggestResponse.suggestions();
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
    
    
    @Test
    public void testFrequentTermsHaveHigherWeight() throws Exception {
        List<Map<String, Object>> products = createProducts("ProductName", "BMW 318", "BMW 318", "BMW 528", "BMW M3",
                "the BMW 320", "VW Jetta", "BMW M3", "BMW M3", "BMW M3", "BMW M3", "BMW M3", "BMW M3");
        indexProducts(products);

        SuggestionQuery query = new SuggestionQuery(index, type, "ProductName.keyword", "b")
                .suggestType("full").analyzer("simple").sortByFrequency().size(10);
        Map<String,Long> suggestions = getWeightedSuggestions(query);
        
        System.out.println(suggestions);
        
        assertTrue(suggestions.get("BMW M3") > suggestions.get("BMW 318"));
        assertTrue(suggestions.get("BMW M3") > suggestions.get("BMW 528"));
        assertTrue(suggestions.get("BMW 318") > suggestions.get("BMW 528"));
        
        assertSuggestions(Lists.newArrayList(suggestions.keySet()), "BMW M3", "BMW 318", "BMW 528");
    }
    
    @Test
    public void testFrequencyWorksWithFuzzy() throws Exception {
       List<Map<String, Object>> products = createProducts("ProductName", "BMW 318", "BMW 318", "BMW 528", "BMW M3",
                "the BMW 320", "VW Jetta", "BMW M3", "BMW M3", "BMW M3", "BMW M3", "BMW M3", "BMW M3");
        indexProducts(products);

        SuggestionQuery query = new SuggestionQuery(index, type, "ProductName.keyword", "b")
                .suggestType("fuzzy").analyzer("standard").sortByFrequency().size(10);
        Map<String,Long> suggestions = getWeightedSuggestions(query);
        
        System.out.println(suggestions);
        
        assertTrue(suggestions.get("BMW M3") > suggestions.get("BMW 318"));
        assertTrue(suggestions.get("BMW M3") > suggestions.get("BMW 528"));
        assertTrue(suggestions.get("BMW 318") > suggestions.get("BMW 528"));
        
        assertSuggestions(Lists.newArrayList(suggestions.keySet()), "BMW M3", "BMW 318", "BMW 528");
    }
    
    @Test
    public void testFrequencyWorksWithStop() throws Exception {
       List<Map<String, Object>> products = createProducts("ProductName", "BMW 318", "BMW 318", "BMW 528", "BMW M3",
                "the BMW 320", "VW Jetta", "BMW M3", "BMW M3", "BMW M3", "BMW M3", "BMW M3", "BMW M3");
        indexProducts(products);

        SuggestionQuery query = new SuggestionQuery(index, type, "ProductName.keyword", "b")
                .suggestType("full").indexAnalyzer("stop").sortByFrequency().queryAnalyzer("stop").size(10);
        Map<String,Long> suggestions = getWeightedSuggestions(query);
        
        System.out.println(suggestions);
        
        assertTrue(suggestions.get("BMW M3") > suggestions.get("BMW 318"));
        assertTrue(suggestions.get("BMW M3") > suggestions.get("BMW 528"));
        assertTrue(suggestions.get("BMW 318") > suggestions.get("BMW 528"));
        
        assertSuggestions(Lists.newArrayList(suggestions.keySet()), "BMW M3", "BMW 318", "BMW 528", "the BMW 320");
    }
}
