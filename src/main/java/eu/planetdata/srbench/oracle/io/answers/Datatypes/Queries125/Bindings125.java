package eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125;

import java.util.Objects;

public class Bindings125 {
    public long timestamp;
    public Binding125 binding;

    public Bindings125(long timestamp, Binding125 binding) {
        this.timestamp = timestamp;
        this.binding = binding;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bindings125 bindings = (Bindings125) o;
        return (this.timestamp == bindings.timestamp) && Objects.equals(binding, bindings.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, binding);
    }
}
