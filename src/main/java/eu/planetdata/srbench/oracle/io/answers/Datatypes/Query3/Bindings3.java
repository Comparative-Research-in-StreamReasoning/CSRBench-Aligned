package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3;

import java.util.Objects;

public class Bindings3 {
    public long timestamp;
    public Binding3 binding;

    public Bindings3(long timestamp, Binding3 binding) {
        this.timestamp = timestamp;
        this.binding = binding;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bindings3 bindings = (Bindings3) o;
        return timestamp == bindings.timestamp && Objects.equals(binding, bindings.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, binding);
    }
}
