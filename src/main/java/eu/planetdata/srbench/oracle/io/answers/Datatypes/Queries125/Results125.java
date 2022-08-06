package eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125;

import java.util.Arrays;

public class Results125 {
    Bindings125[] bindings;

    public Results125(Bindings125[] bindings) {
        this.bindings = bindings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Results125 results = (Results125) o;
        return Arrays.equals(bindings, results.bindings);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bindings);
    }
}
