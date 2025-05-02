package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseResponse;

import java.util.ArrayList;
import java.util.List;

public class FilterResponse extends BaseResponse {
    private static final long serialVersionUID = 401L;
    private final List<String> storeResultStrings;

    public FilterResponse(List<String> results) {
        this.storeResultStrings = results == null ? new ArrayList<>() : results;
    }

    public List<String> getStoreResultStrings() {
        return storeResultStrings;
    }
}
