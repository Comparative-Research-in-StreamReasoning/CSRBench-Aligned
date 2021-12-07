package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query7;

import java.util.Arrays;

public class Head7 {
    public String[] vars;
    public Head7(String[] vars) {
        this.vars = vars;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Head7 head = (Head7) o;
        return Arrays.equals(vars, head.vars);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vars);
    }
}
