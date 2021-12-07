package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query6;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125.TypeValue;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3.TripleTypValue;

import java.util.Objects;

public class Binding6 {
    public TypeValue sensor;
    public TypeValue obs;
    public TypeValue ob1;
    public TripleTypValue value1;

    public Binding6(TypeValue sensor, TypeValue obs, TypeValue ob1, TripleTypValue value1) {
        this.sensor = sensor;
        this.obs = obs;
        this.ob1 = ob1;
        this.value1 = value1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Binding6 binding6 = (Binding6) o;
        return Objects.equals(sensor, binding6.sensor) && Objects.equals(obs, binding6.obs) && Objects.equals(ob1, binding6.ob1) && Objects.equals(value1, binding6.value1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensor, obs, ob1, value1);
    }
}
