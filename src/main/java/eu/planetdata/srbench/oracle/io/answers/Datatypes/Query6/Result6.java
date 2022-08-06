package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query6;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Result;

import java.util.Objects;

public class Result6 extends Result {
    public Head6 head;
    public Results6 results;

    public Result6(Head6 head, long timestamp, Results6 results) {
        this.head = head;
        this.timestamp = timestamp;
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Result6 that = (Result6) o;
        return timestamp == that.timestamp && Objects.equals(head, that.head) && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(head, timestamp, results);
    }
}
