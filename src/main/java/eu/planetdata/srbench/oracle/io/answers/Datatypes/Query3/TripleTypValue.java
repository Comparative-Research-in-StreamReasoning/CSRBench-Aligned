package eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3;

import java.util.Objects;

public class TripleTypValue {
    public String type;
    public String value;
    public String datatype;

    public TripleTypValue(String type, String value, String datatype) {
        this.type = type;
        this.value = value;
        this.datatype = datatype;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TripleTypValue that = (TripleTypValue) o;
        return Objects.equals(type, that.type) && Objects.equals(value, that.value) && Objects.equals(datatype, that.datatype);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value, datatype);
    }
}
