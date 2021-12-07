package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query4;

import java.util.Objects;

public class Bindings4 {
    public long timestamp;
    public Binding4 binding;

    public Bindings4(long timestamp, Binding4 binding) {
        this.timestamp = timestamp;
        this.binding = binding;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bindings4 bindings = (Bindings4) o;
        return timestamp == bindings.timestamp && Objects.equals(binding, bindings.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, binding);
    }
}
