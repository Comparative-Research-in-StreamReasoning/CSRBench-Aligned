package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query4;

import eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3.TripleTypValue;

import java.util.Objects;

public class Binding4 {
    public TripleTypValue avg;

    public Binding4(TripleTypValue avg) {
        this.avg = avg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Binding4 binding4 = (Binding4) o;
        return Objects.equals(avg, binding4.avg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(avg);
    }
}
