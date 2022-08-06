package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query6;

import java.util.Arrays;

public class Head6 {
    public String[] vars;
    public Head6(String[] vars) {
        this.vars = vars;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Head6 head = (Head6) o;
        return Arrays.equals(vars, head.vars);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vars);
    }
}
