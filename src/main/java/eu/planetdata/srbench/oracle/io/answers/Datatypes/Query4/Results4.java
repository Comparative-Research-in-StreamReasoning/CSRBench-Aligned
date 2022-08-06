package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query4;

import java.util.Arrays;

public class Results4 {
    Bindings4[] bindings;

    public Results4(Bindings4[] bindings) {
        this.bindings = bindings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Results4 results = (Results4) o;
        return Arrays.equals(bindings, results.bindings);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bindings);
    }
}
