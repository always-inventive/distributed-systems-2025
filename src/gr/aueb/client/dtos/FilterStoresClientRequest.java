package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseRequest;
import gr.aueb.model.FilterCriteria;

import java.io.Serializable;

/**
 * Client request to filter stores
 */
public class FilterStoresClientRequest extends BaseClientRequest implements Serializable {
    private static final long serialVersionUID = 301L;
    private final FilterCriteria criteria;

    public FilterStoresClientRequest(FilterCriteria criteria) {
        this.criteria = criteria;
    }

    public FilterCriteria getCriteria() {
        return criteria;
    }
}