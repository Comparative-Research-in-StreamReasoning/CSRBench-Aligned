package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3;

import java.util.Arrays;

public class Head3 {
    public String[] vars;
    public Head3(String[] vars) {
        this.vars = vars;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Head3 head = (Head3) o;
        return Arrays.equals(vars, head.vars);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vars);
    }
}
