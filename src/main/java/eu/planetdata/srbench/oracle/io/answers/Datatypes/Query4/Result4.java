package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query4;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Result;

import java.util.Objects;

public class Result4 extends Result {
    public Head4 head;
    public Results4 results;

    public Result4(Head4 head, long timestamp, Results4 results) {
        this.head = head;
        this.timestamp = timestamp;
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Result4 that = (Result4) o;
        return timestamp == that.timestamp && Objects.equals(head, that.head) && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(head, timestamp, results);
    }
}
