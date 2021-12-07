package eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125;

import java.util.Arrays;

public class Head125 {
    public String[] vars;
    public Head125(String[] vars) {
        this.vars = vars;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Head125 head = (Head125) o;
        return Arrays.equals(vars, head.vars);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vars);
    }
}
