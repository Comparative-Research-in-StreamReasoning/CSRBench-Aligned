package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query6;

import java.util.Objects;

public class Bindings6 {
    public long timestamp;
    public Binding6 binding;

    public Bindings6(long timestamp, Binding6 binding) {
        this.timestamp = timestamp;
        this.binding = binding;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bindings6 bindings = (Bindings6) o;
        return timestamp == bindings.timestamp && Objects.equals(binding, bindings.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, binding);
    }
}
