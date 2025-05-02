package gr.aueb.client.dtos;
import gr.aueb.dtos.BaseRequest;
import gr.aueb.model.FilterCriteria;

/**
 * Request from Master to Worker to perform the "Map" phase of filtering stores.
 * Contains the job details, Reducer info, and filter criteria.
 */
public class MapFilterTaskRequest extends BaseRequest {
    private static final long serialVersionUID = 103L; // Keep old ID or assign new one
    private final String jobId;
    private final String reducerHost;
    private final int reducerPort;
    private final FilterCriteria criteria; // Send the criteria object

    public MapFilterTaskRequest(String jobId, String reducerHost, int reducerPort, FilterCriteria criteria) {
        this.jobId = jobId;
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;
        this.criteria = criteria;
    }

    // Getters
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