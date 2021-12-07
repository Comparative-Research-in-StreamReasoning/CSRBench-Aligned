package eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125;

import java.util.Objects;

public class Binding125 {
    public TypeValue sensor;
    public TypeValue obs;

    public Binding125(TypeValue sensor, TypeValue obs) {
        this.sensor = sensor;
        this.obs = obs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Binding125 binding = (Binding125) o;
        return Objects.equals(sensor, binding.sensor) && Objects.equals(obs, binding.obs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensor, obs);
    }
}
