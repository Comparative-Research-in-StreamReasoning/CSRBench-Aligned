package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query7;

import java.util.Objects;

public class Bindings7 {
    public long timestamp;
    public Binding7 binding;

    public Bindings7(long timestamp, Binding7 binding) {
        this.timestamp = timestamp;
        this.binding = binding;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bindings7 bindings = (Bindings7) o;
        return binding.equals(bindings.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, binding);
    }
}
