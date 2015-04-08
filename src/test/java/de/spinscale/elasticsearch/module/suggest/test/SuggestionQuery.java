package de.spinscale.elasticsearch.module.suggest.test;

import java.util.Locale;

public class SuggestionQuery {

    public final String index;
    public final String type;
    public final String field;
    public final String term;
    public String suggestType;
    public String indexAnalyzer;
    public String queryAnalyzer;
    public Integer size;
    public Float similarity;
    public String analyzer;
    public boolean preservePositionIncrements = true;
    public boolean sortByFrequency = false;

    public SuggestionQuery(String index, String type, String field, String term) {
        this.index = index;
        this.type = type;
        this.field = field;
        this.term = term;
    }

    public SuggestionQuery size(Integer size) {
        this.size = size;
        return this;
    }

    public SuggestionQuery similarity(Float similarity) {
        this.similarity = similarity;
        return this;
    }

    public SuggestionQuery suggestType(String suggestType) {
        this.suggestType = suggestType;
        return this;
    }

    public SuggestionQuery analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public SuggestionQuery indexAnalyzer(String indexAnalyzer) {
        this.indexAnalyzer = indexAnalyzer;
        return this;
    }

    public SuggestionQuery queryAnalyzer(String queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "Index [%s] type [%s] field [%s] term [%s]", index, type, field, term));
        if (size != null) sb.append(String.format(Locale.ROOT, " size[%s]", size));
        if (similarity != null) sb.append(String.format(Locale.ROOT, " similarity[%s]", similarity));
        if (suggestType != null) sb.append(String.format(Locale.ROOT, " suggestType[%s]", suggestType));
        if (analyzer != null) sb.append(String.format(Locale.ROOT, " analyzer[%s]", analyzer));
        if (indexAnalyzer!= null) sb.append(String.format(Locale.ROOT, " indexAnalyzer[%s]", indexAnalyzer));
        if (queryAnalyzer != null) sb.append(String.format(Locale.ROOT, " queryAnalyzer[%s]", queryAnalyzer));
        sb.append(String.format(Locale.ROOT, " preservePositionIncrements[%s]", preservePositionIncrements));
        if (sortByFrequency) sb.append(" sorting by frequency");
        else sb.append(" sorting alphabetically");
        return sb.toString();
    }

    public SuggestionQuery preservePositionIncrements(boolean preservePositionIncrements) {
        this.preservePositionIncrements = preservePositionIncrements;
        return this;
    }
    
    public SuggestionQuery sortByFrequency() {
    	this.sortByFrequency = true;
    	return this;
    }
    
    public SuggestionQuery sortAlphabetically() {
    	this.sortByFrequency = false;
    	return this;
    }
}
