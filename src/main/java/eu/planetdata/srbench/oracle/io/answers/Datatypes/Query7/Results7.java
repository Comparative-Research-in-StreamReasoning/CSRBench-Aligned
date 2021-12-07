package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query7;

import java.util.Arrays;

public class Results7 {
    Bindings7[] bindings;

    public Results7(Bindings7[] bindings) {
        this.bindings = bindings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Results7 results = (Results7) o;
        return Arrays.equals(bindings, results.bindings);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bindings);
    }
}
