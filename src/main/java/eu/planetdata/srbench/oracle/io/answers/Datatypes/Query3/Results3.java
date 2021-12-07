package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3;

import java.util.Arrays;

public class Results3 {
    Bindings3[] bindings;

    public Results3(Bindings3[] bindings) {
        this.bindings = bindings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Results3 results = (Results3) o;
        return Arrays.equals(bindings, results.bindings);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bindings);
    }
}
