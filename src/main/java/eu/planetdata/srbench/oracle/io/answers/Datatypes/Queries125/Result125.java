package eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Result;

import java.util.Objects;

public class Result125 extends Result {
    public Head125 head;
    public Results125 results;

    public Result125(Head125 head, long timestamp, Results125 results) {
        this.head = head;
        this.timestamp = timestamp;
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Result125 that = (Result125) o;
        return (this.timestamp == that.timestamp) && Objects.equals(head, that.head) && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(head, timestamp, results);
    }
}
