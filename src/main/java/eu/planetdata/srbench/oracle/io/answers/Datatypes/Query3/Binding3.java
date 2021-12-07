package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125.TypeValue;

import java.util.Objects;

public class Binding3 {
    public TypeValue sensor;
    public TypeValue obs;
    public TripleTypValue value;

    public Binding3(TypeValue sensor, TypeValue obs, TripleTypValue value) {
        this.sensor = sensor;
        this.obs = obs;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Binding3 binding3 = (Binding3) o;
        return Objects.equals(sensor, binding3.sensor) && Objects.equals(obs, binding3.obs) && Objects.equals(value, binding3.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensor, obs, value);
    }
}
