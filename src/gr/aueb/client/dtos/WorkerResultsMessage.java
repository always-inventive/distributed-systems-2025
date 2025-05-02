package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseReducerMessage;

import java.util.ArrayList;
import java.util.List;

public class WorkerResultsMessage extends BaseReducerMessage {
    private static final long serialVersionUID = 501L;
    private final List<String> results; // Send results as strings

    public WorkerResultsMessage(String jobId, List<String> results) {
        super(jobId);
        this.results = results == null ? new ArrayList<>() : results;
    }

    public List<String> getResults() {
        return results;
    }
}
