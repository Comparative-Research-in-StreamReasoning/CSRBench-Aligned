package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query4;

import java.util.Arrays;

public class Head4 {
    public String[] vars;
    public Head4(String[] vars) {
        this.vars = vars;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Head4 head = (Head4) o;
        return Arrays.equals(vars, head.vars);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vars);
    }
}
