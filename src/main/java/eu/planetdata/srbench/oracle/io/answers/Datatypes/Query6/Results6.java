package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query6;

import java.util.Arrays;

public class Results6 {
    Bindings6[] bindings;

    public Results6(Bindings6[] bindings) {
        this.bindings = bindings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Results6 results = (Results6) o;
        return Arrays.equals(bindings, results.bindings);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bindings);
    }
}
