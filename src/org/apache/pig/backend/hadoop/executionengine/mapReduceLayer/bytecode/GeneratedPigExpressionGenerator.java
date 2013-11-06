package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GeneratedPigExpressionGenerator {

    static String method(BinaryOperator op, Type type) {
        return op.name() + "_" + type.name();
    }

    static enum Type {
        BOOLEAN("boolean", "Boolean") {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE: case BAG: case MAP: case DATETIME:
                    return castError(t);
                case BIGINTEGER: case BIGDECIMAL:
                    return "return !" + t.type + ".ZERO.equals(v)";
                case CHARARRAY:
                    return "return v.equalsIgnoreCase(\"true\")";
                case INT: case LONG: case FLOAT: case DOUBLE:
                    return "return v != 0";
                default:
                    return super.castFrom(t);
                }
            }
        },
        INT("int", "Integer") {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case MAP:
                case BAG:
                    return castError(t);
                case CHARARRAY:
                    return "return Integer.parseInt(v)";
                case DATETIME:
                    return "return (int)v.getMillis()";
                case BIGINTEGER: case BIGDECIMAL:
                    return "return v.intValue()";
                default:
                    return super.castFrom(t);
                }
            }
        },
        LONG("long", "Long") {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case MAP:
                case BAG:
                    return castError(t);
                case CHARARRAY:
                    return "return Long.parseLong(v)";
                case DATETIME:
                    return "return v.getMillis()";
                case BIGINTEGER: case BIGDECIMAL:
                    return "return v.longValue()";
                default:
                    return super.castFrom(t);
                }
            }
        },
        FLOAT("float", "Float") {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case MAP:
                case BAG:
                    return castError(t);
                case CHARARRAY:
                    return "return Float.parseFloat(v)";
                case DATETIME:
                    return "return (float)v.getMillis()";
                case BIGINTEGER: case BIGDECIMAL:
                    return "return v.floatValue()";
                default:
                    return super.castFrom(t);
                }
            }
        },
        DOUBLE("double", "Double") {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case MAP:
                case BAG:
                    return castError(t);
                case CHARARRAY:
                    return "return Double.parseDouble(v)";
                case DATETIME:
                    return "return (double)v.getMillis()";
                case BIGINTEGER: case BIGDECIMAL:
                    return "return v.doubleValue()";
                default:
                    return super.castFrom(t);
                }
            }
        },
        CHARARRAY("String", true) {
            public String castFrom(Type t) {
                switch (t) {
                case CHARARRAY:
                    return super.castFrom(t);
                default:
                    return "return String.valueOf(v)";
                }
            }
        },
        BAG("org.apache.pig.data.DataBag", true) {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case MAP:
                case INT:
                case DOUBLE:
                case LONG:
                case FLOAT:
                case DATETIME:
                case CHARARRAY:
                case BOOLEAN:
                case BIGINTEGER:
                case BIGDECIMAL:
                    return castError(t);
                default:
                    return super.castFrom(t);
                }
            }
        },
        DATETIME("org.joda.time.DateTime", true) {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case MAP:
                case BAG:
                case BOOLEAN:
                    return castError(t);
                case BIGINTEGER: case BIGDECIMAL:
                    return "return new org.joda.time.DateTime(v.longValue())";
                case CHARARRAY:
                    return "return new org.joda.time.DateTime(Long.valueOf(v))";
                case INT: case LONG: case FLOAT: case DOUBLE:
                    return "return new org.joda.time.DateTime((long)v)";
                default:
                    return super.castFrom(t);
                }
            }
        },
        TUPLE("org.apache.pig.data.Tuple", true) {
            public String castFrom(Type t) {
                switch (t) {
                case MAP:
                case BAG:
                case INT:
                case DOUBLE:
                case LONG:
                case FLOAT:
                case DATETIME:
                case CHARARRAY:
                case BOOLEAN:
                case BIGINTEGER:
                case BIGDECIMAL:
                    return castError(t);
                default:
                    return super.castFrom(t);
                }
            }
        },
        MAP("java.util.Map", true) {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case BAG:
                case INT:
                case DOUBLE:
                case LONG:
                case FLOAT:
                case DATETIME:
                case CHARARRAY:
                case BOOLEAN:
                case BIGINTEGER:
                case BIGDECIMAL:
                    return castError(t);
                default:
                    return super.castFrom(t);
                }
            }
        },
        BYTEARRAY("org.apache.pig.data.DataByteArray", true) {
            public String castFrom(Type t) {
                return castError(t);
            }
        },
        BIGINTEGER("java.math.BigInteger", true) {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case BAG:
                case MAP:
                    return castError(t);
                case BOOLEAN:
                    return "return (v ? java.math.BigInteger.ONE : java.math.BigInteger.ZERO)";
                case BIGDECIMAL:
                    return "return java.math.BigInteger.valueOf(v.longValue())";
                case CHARARRAY:
                    return "return new java.math.BigInteger(v)";
                case INT: case LONG: case FLOAT: case DOUBLE:
                    return "return java.math.BigInteger.valueOf((long)v)";
                case DATETIME:
                    return "return java.math.BigInteger.valueOf(v.getMillis())";
                default:
                    return super.castFrom(t);
                }
            }
        },

        BIGDECIMAL("java.math.BigDecimal", true) {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE:
                case BAG:
                case MAP:
                    return castError(t);
                case BOOLEAN:
                    return "return (v ? java.math.BigDecimal.ONE : java.math.BigDecimal.ZERO)";
                case BIGINTEGER:
                    return "return java.math.BigDecimal.valueOf(v.longValue())";
                case CHARARRAY:
                    return "return new java.math.BigDecimal(v)";
                case INT: case LONG: case FLOAT: case DOUBLE:
                    return "return java.math.BigDecimal.valueOf(v)";
                case DATETIME:
                    return "return java.math.BigDecimal.valueOf(v.getMillis())";
                default:
                    return super.castFrom(t);
                }
            }
        };

        public static final List<Type> numeric = Arrays.asList(INT, LONG, FLOAT, DOUBLE);

        public static final List<Type> comparableTypes = Arrays.asList(INT, LONG, FLOAT, DOUBLE, CHARARRAY);

        public final String type;

        public final boolean comparable;

        public final String boxed;

        Type(String type) {
            this(type, type, false);
        }

        Type(String type, String boxed) {
            this(type, boxed, false);
        }

        Type(String type, boolean comparable) {
            this(type, type, comparable);
        }

        Type(String type, String boxed, boolean comparable) {
            this.type = type;
            this.boxed = boxed;
            this.comparable = comparable;
        }

        public String op(CompOperator op) {
            return op.op(BOOLEAN);
        }

        public String castFrom(Type t) {
            if (t == this) {
                return "return v";
            } else if (t == BYTEARRAY) {
                return castError(t);
            } else if (t == BOOLEAN) {
                if (this == CHARARRAY) {
                    return "return (v ? \"true\" : \"false\")";
                }
                return "return (v ? 1 : 0)";
            }
            return "return (" + this.type + ")v";
        }

        public String castError(Type t) {
            return "throw new IllegalArgumentException(\"cast " + t + " to " + this + "\")";
        }
    }

    static interface BinaryOperator {

        String op(Type type);

        String name();

        Type returnType(Type type);

    }

    static enum BooleanOperator implements BinaryOperator {
        AND("&&"), OR("||"), XOR("^");

        private final String op;

        BooleanOperator(String op) {
            this.op = op;
        }

        @Override
        public String op(Type type) {
            return "l " + op + " r";
        }

        @Override
        public Type returnType(Type type) {
            return Type.BOOLEAN;
        }

    }

    static enum ArythmeticOperator implements BinaryOperator {
        PLUS("+"), MINUS("-"), MUL("*"), DIV("/");

        private final String op;

        ArythmeticOperator(String op) {
            this.op = op;
        }

        @Override
        public String op(Type type) {
            return "l " + op + " r";
        }

        @Override
        public Type returnType(Type type) {
            return type;
        }
    }

    static enum CompOperator implements BinaryOperator {
        GT(">"), LT("<"), EQ("=="), GE(">="), LE("<="), NEQ("!=");

        private final String op;

        CompOperator(String op) {
            this.op = op;
        }

        @Override
        public String op(Type type) {
            if (type.comparable) {
                return "l.compareTo(r) " + op + " 0";
            } else {
                return "l " + op + " r";
            }
        }

        @Override
        public Type returnType(Type type) {
            return Type.BOOLEAN;
        }
    }


    public static void main(String[] args) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;\n");
        //sb.append("import java.util.*;");
        sb.append("public abstract class GeneratedPigExpression {\n");
        sb.append("  // abstract methods\n");
        generateAbstracts(sb, Type.values());
        sb.append("  // boolean operators\n");
        generate(sb, BooleanOperator.values(), Arrays.asList(Type.BOOLEAN));
        sb.append("  // arithmetic operators\n");
        generate(sb, ArythmeticOperator.values(), Type.numeric);
        sb.append("  // comparison operators\n");
        generate(sb, CompOperator.values(), Type.comparableTypes);
        sb.append("  // cast operators\n");
        generateCast(sb, Type.values(), Type.values());
        sb.append("  // projection operators\n");
        generateProj(sb, Type.values());
        sb.append("}\n");
        String code = sb.toString();
        System.out.println(code);

        File f = new File("src/org/apache/pig/backend/hadoop/executionengine/mapReduceLayer/bytecode/GeneratedPigExpression.java");
        final FileWriter w = new FileWriter(f);
        w.write(code);
        w.close();
    }


    private static void generateProj(StringBuilder sb, Type[] types) {
        for (Type type : types) {
            sb.append("  public ").append(type.type).append(" proj_").append(type)
                .append("(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {\n");
            sb.append("    return (" + type.boxed + ")t.get(i);\n");
            sb.append("  }\n");
            sb.append("\n");
        }
    }


    private static void generateAbstracts(StringBuilder sb, Type[] types) {
        for (Type type : types) {
            sb.append("  public abstract ").append(type.type).append(" ")
                .append("eval").append(type).append("(org.apache.pig.data.Tuple t);\n");
            sb.append("\n");
        }
    }


    private static void generateCast(StringBuilder sb, Type[] from, Type[] to) {
        for (Type t : to) {
            for (Type f : from) {
                sb.append("  public ").append(t.type).append(" cast_").append(f).append("_to_").append(t).append("(").append(f.type).append(" v){\n");
                sb.append("    ").append(t.castFrom(f)).append(";\n");
                sb.append("  }\n");
                sb.append("\n");
            }
        }
    }


    private static void generate(StringBuilder sb, BinaryOperator[] operators, List<Type> types) {
        for (Type type : types) {
            for (BinaryOperator op : operators) {
                sb.append("  public ").append(op.returnType(type).type).append(" ")
                    .append(method(op, type))
                        .append("(").append(type.type).append(" l, ").append(type.type).append(" r) {\n");
                sb.append("    return " + op.op(type) + ";\n");
                sb.append("  }\n");
                sb.append("\n");
            }
        }
    }
}
