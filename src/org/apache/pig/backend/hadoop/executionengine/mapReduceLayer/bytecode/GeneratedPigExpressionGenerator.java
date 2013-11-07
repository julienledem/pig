package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.data.DataType;

public class GeneratedPigExpressionGenerator {

    static String method(BinaryOperator op, Type type) {
        return op.name() + "_" + type.name();
    }

    static enum Type {
        BOOLEAN(Boolean.TYPE, Boolean.class, DataType.BOOLEAN) {
            public String castFrom(Type t) {
                switch (t) {
                case TUPLE: case BAG: case MAP: case DATETIME:
                    return castError(t);
                case BIGINTEGER: case BIGDECIMAL:
                    return "return !" + t.type.getName() + ".ZERO.equals(v)";
                case CHARARRAY:
                    return "return v.equalsIgnoreCase(\"true\")";
                case INT: case LONG: case FLOAT: case DOUBLE:
                    return "return v != 0";
                default:
                    return super.castFrom(t);
                }
            }
        },
        INT(Integer.TYPE, Integer.class, DataType.INTEGER) {
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
        LONG(Long.TYPE, Long.class, DataType.LONG) {
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
        FLOAT(Float.TYPE, Float.class, DataType.FLOAT) {
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
        DOUBLE(Double.TYPE, Double.class, DataType.DOUBLE) {
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
        CHARARRAY(String.class, true, DataType.CHARARRAY) {
            public String castFrom(Type t) {
                switch (t) {
                case CHARARRAY:
                    return super.castFrom(t);
                default:
                    return "return String.valueOf(v)";
                }
            }
        },
        BAG(org.apache.pig.data.DataBag.class, true, DataType.BAG) {
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
        DATETIME(org.joda.time.DateTime.class, true, DataType.DATETIME) {
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
        TUPLE(org.apache.pig.data.Tuple.class, true, DataType.TUPLE) {
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
        MAP(java.util.Map.class, true, DataType.MAP) {
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
        BYTEARRAY(org.apache.pig.data.DataByteArray.class, true, DataType.BYTEARRAY) {
            public String castFrom(Type t) {
                return castError(t);
            }
        },
        BIGINTEGER(java.math.BigInteger.class, true, DataType.BIGINTEGER) {
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

        BIGDECIMAL(java.math.BigDecimal.class, true, DataType.BIGDECIMAL) {
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

        public final Class<?> type;

        public final boolean comparable;

        public final Class<?> boxed;

        public final byte pigType;

        Type(Class<?> type, byte pigType) {
            this(type, type, false, pigType);
        }

        Type(Class<?> type, Class<?> boxed, byte pigType) {
            this(type, boxed, false, pigType);
        }

        Type(Class<?> type, boolean comparable, byte pigType) {
            this(type, type, comparable, pigType);
        }

        Type(Class<?> type, Class<?> boxed, boolean comparable, byte pigType) {
            this.type = type;
            this.boxed = boxed;
            this.comparable = comparable;
            this.pigType = pigType;
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
            return "return (" + this.type.getName() + ")v";
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

    static enum ArithmeticOperator implements BinaryOperator {
        PLUS("+"), MINUS("-"), MUL("*"), DIV("/");

        private final String op;

        ArithmeticOperator(String op) {
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
        generate(sb, ArithmeticOperator.values(), Type.numeric);
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
            sb.append("  public ").append(type.type.getName()).append(" proj_").append(type)
                .append("(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {\n");
            sb.append("    return (" + type.boxed.getName() + ")t.get(i);\n");
            sb.append("  }\n");
            sb.append("\n");
        }
    }


    private static void generateAbstracts(StringBuilder sb, Type[] types) {
        for (Type type : types) {
            sb.append("  public abstract ").append(type.type.getName()).append(" ")
                .append("eval").append(type).append("(org.apache.pig.data.Tuple t);\n");
            sb.append("\n");
        }
    }


    private static void generateCast(StringBuilder sb, Type[] from, Type[] to) {
        for (Type t : to) {
            for (Type f : from) {
                sb.append("  public ").append(t.type.getName()).append(" cast_").append(f).append("_to_").append(t).append("(").append(f.type.getName()).append(" v){\n");
                sb.append("    ").append(t.castFrom(f)).append(";\n");
                sb.append("  }\n");
                sb.append("\n");
            }
        }
    }


    private static void generate(StringBuilder sb, BinaryOperator[] operators, List<Type> types) {
        for (Type type : types) {
            for (BinaryOperator op : operators) {
                sb.append("  public ").append(op.returnType(type).type.getName()).append(" ")
                    .append(method(op, type))
                        .append("(").append(type.type.getName()).append(" l, ").append(type.type.getName()).append(" r) {\n");
                sb.append("    return " + op.op(type) + ";\n");
                sb.append("  }\n");
                sb.append("\n");
            }
        }
    }
}
