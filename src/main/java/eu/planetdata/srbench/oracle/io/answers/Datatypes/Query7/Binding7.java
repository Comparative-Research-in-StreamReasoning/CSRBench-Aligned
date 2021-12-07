package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query7;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125.TypeValue;

import java.util.Objects;

public class Binding7 {
    public TypeValue sensor;
    public TypeValue ob1;

    public Binding7(TypeValue sensor, TypeValue ob1) {
        this.sensor = sensor;
        this.ob1 = ob1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Binding7 binding7 = (Binding7) o;
        return sensor.equals(binding7.sensor) && ob1.equals(binding7.ob1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensor, ob1);
    }
}
