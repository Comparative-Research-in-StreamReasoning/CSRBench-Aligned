package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Result;

import java.util.Objects;

public class Result3 extends Result {
    public Head3 head;
    public Results3 results;

    public Result3(Head3 head, long timestamp, Results3 results) {
        this.head = head;
        this.timestamp = timestamp;
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Result3 that = (Result3) o;
        return timestamp == that.timestamp && Objects.equals(head, that.head) && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(head, timestamp, results);
    }
}
