package eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125;

import java.util.Objects;

public class TypeValue {
    public String type;
    public String value;

    public TypeValue(String type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeValue typeValue = (TypeValue) o;
        return type.equals(typeValue.type) && value.equals(typeValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }
}
