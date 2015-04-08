package de.spinscale.elasticsearch.rest.action.suggest;

import de.spinscale.elasticsearch.action.suggest.suggest.SuggestAction;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestRequest;
import de.spinscale.elasticsearch.action.suggest.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

public class RestSuggestAction extends BaseRestHandler {

    @Inject
    public RestSuggestAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/__suggest", this);
        controller.registerHandler(GET, "/{index}/{type}/__suggest", this);
        controller.registerHandler(POST, "/{index}/__suggest", this);
        controller.registerHandler(POST, "/{index}/{type}/__suggest", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));

        try {
            Map<String, Object> parserMap = null;
            if (request.hasContent()) {
                XContentParser parser = XContentFactory.xContent(request.content()).createParser(request.content());
                parserMap = parser.mapAndClose();
            } else if (request.hasParam("source")) {
                String source = request.param("source");
                XContentParser parser = XContentFactory.xContent(source).createParser(source);
                parserMap = parser.mapAndClose();
            } else {
                channel.sendResponse(new BytesRestResponse(BAD_REQUEST, "Please provide body data or source parameter"));
            }

            SuggestRequest suggestRequest = new SuggestRequest(indices);
            suggestRequest.field(XContentMapValues.nodeStringValue(parserMap.get("field"), ""));
            suggestRequest.suggestType(XContentMapValues.nodeStringValue(parserMap.get("type"), ""));
            if (parserMap.containsKey("analyzer")) {
                suggestRequest.indexAnalyzer(XContentMapValues.nodeStringValue(parserMap.get("analyzer"), ""));
                suggestRequest.queryAnalyzer(XContentMapValues.nodeStringValue(parserMap.get("analyzer"), ""));
            } else {
                suggestRequest.indexAnalyzer(XContentMapValues.nodeStringValue(parserMap.get("indexAnalyzer"), ""));
                suggestRequest.queryAnalyzer(XContentMapValues.nodeStringValue(parserMap.get("queryAnalyzer"), ""));
            }
            suggestRequest.term(XContentMapValues.nodeStringValue(parserMap.get("term"), ""));
            suggestRequest.similarity(XContentMapValues.nodeFloatValue(parserMap.get("similarity"), 1.0f));
            suggestRequest.size(XContentMapValues.nodeIntegerValue(parserMap.get("size"), 10));

            if (parserMap.containsKey("sortByFrequency"))
            	suggestRequest.sortByFrequency(XContentMapValues.nodeBooleanValue(parserMap.get("sortByFrequency")));
            // TODO the other boolean is missing here as well?
            
            client.execute(SuggestAction.INSTANCE, suggestRequest, new RestToXContentListener<SuggestResponse>(channel));
        } catch (IOException e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }
}
