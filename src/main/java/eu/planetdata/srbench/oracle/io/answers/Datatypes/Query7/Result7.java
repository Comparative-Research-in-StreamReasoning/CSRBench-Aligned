package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query7;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Result;

import java.util.Objects;

public class Result7 extends Result {
    public Head7 head;
    public Results7 results;

    public Result7(Head7 head, long timestamp, Results7 results) {
        this.head = head;
        this.timestamp = timestamp;
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Result7 result7 = (Result7) o;
        return head.equals(result7.head) && results.equals(result7.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(head, results);
    }
}
