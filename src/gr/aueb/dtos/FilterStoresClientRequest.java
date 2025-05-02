package gr.aueb.dtos;

import gr.aueb.model.FilterCriteria;

/**
 * Request for a worker to process filters for a MapReduce job.
 */
public class FilterStoresClientRequest extends BaseRequest {
    private static final long serialVersionUID = 103L;
    private final String jobId;
    private final String reducerHost;
    private final int reducerPort;
    private final FilterCriteria criteria; // Send the criteria object

    public FilterStoresClientRequest(String jobId, String reducerHost, int reducerPort, FilterCriteria criteria) {
        this.jobId = jobId;
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;
        this.criteria = criteria;
    }

    public String getJobId() {
        return jobId;
    }

    public String getReducerHost() {
        return reducerHost;
    }

    public int getReducerPort() {
        return reducerPort;
    }

    public FilterCriteria getCriteria() {
        return criteria;
    }
}
