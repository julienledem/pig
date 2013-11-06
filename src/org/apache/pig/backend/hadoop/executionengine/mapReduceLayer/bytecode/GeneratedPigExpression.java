package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;
public abstract class GeneratedPigExpression {
  // abstract methods
  public abstract boolean evalBOOLEAN(org.apache.pig.data.Tuple t);

  public abstract int evalINT(org.apache.pig.data.Tuple t);

  public abstract long evalLONG(org.apache.pig.data.Tuple t);

  public abstract float evalFLOAT(org.apache.pig.data.Tuple t);

  public abstract double evalDOUBLE(org.apache.pig.data.Tuple t);

  public abstract String evalCHARARRAY(org.apache.pig.data.Tuple t);

  public abstract org.apache.pig.data.DataBag evalBAG(org.apache.pig.data.Tuple t);

  public abstract org.joda.time.DateTime evalDATETIME(org.apache.pig.data.Tuple t);

  public abstract org.apache.pig.data.Tuple evalTUPLE(org.apache.pig.data.Tuple t);

  public abstract java.util.Map evalMAP(org.apache.pig.data.Tuple t);

  public abstract org.apache.pig.data.DataByteArray evalBYTEARRAY(org.apache.pig.data.Tuple t);

  public abstract java.math.BigInteger evalBIGINTEGER(org.apache.pig.data.Tuple t);

  public abstract java.math.BigDecimal evalBIGDECIMAL(org.apache.pig.data.Tuple t);

  // boolean operators
  public boolean AND_BOOLEAN(boolean l, boolean r) {
    return l && r;
  }

  public boolean OR_BOOLEAN(boolean l, boolean r) {
    return l || r;
  }

  public boolean XOR_BOOLEAN(boolean l, boolean r) {
    return l ^ r;
  }

  // arithmetic operators
  public int PLUS_INT(int l, int r) {
    return l + r;
  }

  public int MINUS_INT(int l, int r) {
    return l - r;
  }

  public int MUL_INT(int l, int r) {
    return l * r;
  }

  public int DIV_INT(int l, int r) {
    return l / r;
  }

  public long PLUS_LONG(long l, long r) {
    return l + r;
  }

  public long MINUS_LONG(long l, long r) {
    return l - r;
  }

  public long MUL_LONG(long l, long r) {
    return l * r;
  }

  public long DIV_LONG(long l, long r) {
    return l / r;
  }

  public float PLUS_FLOAT(float l, float r) {
    return l + r;
  }

  public float MINUS_FLOAT(float l, float r) {
    return l - r;
  }

  public float MUL_FLOAT(float l, float r) {
    return l * r;
  }

  public float DIV_FLOAT(float l, float r) {
    return l / r;
  }

  public double PLUS_DOUBLE(double l, double r) {
    return l + r;
  }

  public double MINUS_DOUBLE(double l, double r) {
    return l - r;
  }

  public double MUL_DOUBLE(double l, double r) {
    return l * r;
  }

  public double DIV_DOUBLE(double l, double r) {
    return l / r;
  }

  // comparison operators
  public boolean GT_INT(int l, int r) {
    return l > r;
  }

  public boolean LT_INT(int l, int r) {
    return l < r;
  }

  public boolean EQ_INT(int l, int r) {
    return l == r;
  }

  public boolean GE_INT(int l, int r) {
    return l >= r;
  }

  public boolean LE_INT(int l, int r) {
    return l <= r;
  }

  public boolean NEQ_INT(int l, int r) {
    return l != r;
  }

  public boolean GT_LONG(long l, long r) {
    return l > r;
  }

  public boolean LT_LONG(long l, long r) {
    return l < r;
  }

  public boolean EQ_LONG(long l, long r) {
    return l == r;
  }

  public boolean GE_LONG(long l, long r) {
    return l >= r;
  }

  public boolean LE_LONG(long l, long r) {
    return l <= r;
  }

  public boolean NEQ_LONG(long l, long r) {
    return l != r;
  }

  public boolean GT_FLOAT(float l, float r) {
    return l > r;
  }

  public boolean LT_FLOAT(float l, float r) {
    return l < r;
  }

  public boolean EQ_FLOAT(float l, float r) {
    return l == r;
  }

  public boolean GE_FLOAT(float l, float r) {
    return l >= r;
  }

  public boolean LE_FLOAT(float l, float r) {
    return l <= r;
  }

  public boolean NEQ_FLOAT(float l, float r) {
    return l != r;
  }

  public boolean GT_DOUBLE(double l, double r) {
    return l > r;
  }

  public boolean LT_DOUBLE(double l, double r) {
    return l < r;
  }

  public boolean EQ_DOUBLE(double l, double r) {
    return l == r;
  }

  public boolean GE_DOUBLE(double l, double r) {
    return l >= r;
  }

  public boolean LE_DOUBLE(double l, double r) {
    return l <= r;
  }

  public boolean NEQ_DOUBLE(double l, double r) {
    return l != r;
  }

  public boolean GT_CHARARRAY(String l, String r) {
    return l.compareTo(r) > 0;
  }

  public boolean LT_CHARARRAY(String l, String r) {
    return l.compareTo(r) < 0;
  }

  public boolean EQ_CHARARRAY(String l, String r) {
    return l.compareTo(r) == 0;
  }

  public boolean GE_CHARARRAY(String l, String r) {
    return l.compareTo(r) >= 0;
  }

  public boolean LE_CHARARRAY(String l, String r) {
    return l.compareTo(r) <= 0;
  }

  public boolean NEQ_CHARARRAY(String l, String r) {
    return l.compareTo(r) != 0;
  }

  // cast operators
  public boolean cast_BOOLEAN_to_BOOLEAN(boolean v){
    return v;
  }

  public boolean cast_INT_to_BOOLEAN(int v){
    return v != 0;
  }

  public boolean cast_LONG_to_BOOLEAN(long v){
    return v != 0;
  }

  public boolean cast_FLOAT_to_BOOLEAN(float v){
    return v != 0;
  }

  public boolean cast_DOUBLE_to_BOOLEAN(double v){
    return v != 0;
  }

  public boolean cast_CHARARRAY_to_BOOLEAN(String v){
    return v.equalsIgnoreCase("true");
  }

  public boolean cast_BAG_to_BOOLEAN(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to BOOLEAN");
  }

  public boolean cast_DATETIME_to_BOOLEAN(org.joda.time.DateTime v){
    throw new IllegalArgumentException("cast DATETIME to BOOLEAN");
  }

  public boolean cast_TUPLE_to_BOOLEAN(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to BOOLEAN");
  }

  public boolean cast_MAP_to_BOOLEAN(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to BOOLEAN");
  }

  public boolean cast_BYTEARRAY_to_BOOLEAN(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to BOOLEAN");
  }

  public boolean cast_BIGINTEGER_to_BOOLEAN(java.math.BigInteger v){
    return !java.math.BigInteger.ZERO.equals(v);
  }

  public boolean cast_BIGDECIMAL_to_BOOLEAN(java.math.BigDecimal v){
    return !java.math.BigDecimal.ZERO.equals(v);
  }

  public int cast_BOOLEAN_to_INT(boolean v){
    return (v ? 1 : 0);
  }

  public int cast_INT_to_INT(int v){
    return v;
  }

  public int cast_LONG_to_INT(long v){
    return (int)v;
  }

  public int cast_FLOAT_to_INT(float v){
    return (int)v;
  }

  public int cast_DOUBLE_to_INT(double v){
    return (int)v;
  }

  public int cast_CHARARRAY_to_INT(String v){
    return Integer.parseInt(v);
  }

  public int cast_BAG_to_INT(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to INT");
  }

  public int cast_DATETIME_to_INT(org.joda.time.DateTime v){
    return (int)v.getMillis();
  }

  public int cast_TUPLE_to_INT(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to INT");
  }

  public int cast_MAP_to_INT(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to INT");
  }

  public int cast_BYTEARRAY_to_INT(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to INT");
  }

  public int cast_BIGINTEGER_to_INT(java.math.BigInteger v){
    return v.intValue();
  }

  public int cast_BIGDECIMAL_to_INT(java.math.BigDecimal v){
    return v.intValue();
  }

  public long cast_BOOLEAN_to_LONG(boolean v){
    return (v ? 1 : 0);
  }

  public long cast_INT_to_LONG(int v){
    return (long)v;
  }

  public long cast_LONG_to_LONG(long v){
    return v;
  }

  public long cast_FLOAT_to_LONG(float v){
    return (long)v;
  }

  public long cast_DOUBLE_to_LONG(double v){
    return (long)v;
  }

  public long cast_CHARARRAY_to_LONG(String v){
    return Long.parseLong(v);
  }

  public long cast_BAG_to_LONG(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to LONG");
  }

  public long cast_DATETIME_to_LONG(org.joda.time.DateTime v){
    return v.getMillis();
  }

  public long cast_TUPLE_to_LONG(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to LONG");
  }

  public long cast_MAP_to_LONG(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to LONG");
  }

  public long cast_BYTEARRAY_to_LONG(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to LONG");
  }

  public long cast_BIGINTEGER_to_LONG(java.math.BigInteger v){
    return v.longValue();
  }

  public long cast_BIGDECIMAL_to_LONG(java.math.BigDecimal v){
    return v.longValue();
  }

  public float cast_BOOLEAN_to_FLOAT(boolean v){
    return (v ? 1 : 0);
  }

  public float cast_INT_to_FLOAT(int v){
    return (float)v;
  }

  public float cast_LONG_to_FLOAT(long v){
    return (float)v;
  }

  public float cast_FLOAT_to_FLOAT(float v){
    return v;
  }

  public float cast_DOUBLE_to_FLOAT(double v){
    return (float)v;
  }

  public float cast_CHARARRAY_to_FLOAT(String v){
    return Float.parseFloat(v);
  }

  public float cast_BAG_to_FLOAT(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to FLOAT");
  }

  public float cast_DATETIME_to_FLOAT(org.joda.time.DateTime v){
    return (float)v.getMillis();
  }

  public float cast_TUPLE_to_FLOAT(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to FLOAT");
  }

  public float cast_MAP_to_FLOAT(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to FLOAT");
  }

  public float cast_BYTEARRAY_to_FLOAT(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to FLOAT");
  }

  public float cast_BIGINTEGER_to_FLOAT(java.math.BigInteger v){
    return v.floatValue();
  }

  public float cast_BIGDECIMAL_to_FLOAT(java.math.BigDecimal v){
    return v.floatValue();
  }

  public double cast_BOOLEAN_to_DOUBLE(boolean v){
    return (v ? 1 : 0);
  }

  public double cast_INT_to_DOUBLE(int v){
    return (double)v;
  }

  public double cast_LONG_to_DOUBLE(long v){
    return (double)v;
  }

  public double cast_FLOAT_to_DOUBLE(float v){
    return (double)v;
  }

  public double cast_DOUBLE_to_DOUBLE(double v){
    return v;
  }

  public double cast_CHARARRAY_to_DOUBLE(String v){
    return Double.parseDouble(v);
  }

  public double cast_BAG_to_DOUBLE(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to DOUBLE");
  }

  public double cast_DATETIME_to_DOUBLE(org.joda.time.DateTime v){
    return (double)v.getMillis();
  }

  public double cast_TUPLE_to_DOUBLE(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to DOUBLE");
  }

  public double cast_MAP_to_DOUBLE(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to DOUBLE");
  }

  public double cast_BYTEARRAY_to_DOUBLE(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to DOUBLE");
  }

  public double cast_BIGINTEGER_to_DOUBLE(java.math.BigInteger v){
    return v.doubleValue();
  }

  public double cast_BIGDECIMAL_to_DOUBLE(java.math.BigDecimal v){
    return v.doubleValue();
  }

  public String cast_BOOLEAN_to_CHARARRAY(boolean v){
    return String.valueOf(v);
  }

  public String cast_INT_to_CHARARRAY(int v){
    return String.valueOf(v);
  }

  public String cast_LONG_to_CHARARRAY(long v){
    return String.valueOf(v);
  }

  public String cast_FLOAT_to_CHARARRAY(float v){
    return String.valueOf(v);
  }

  public String cast_DOUBLE_to_CHARARRAY(double v){
    return String.valueOf(v);
  }

  public String cast_CHARARRAY_to_CHARARRAY(String v){
    return v;
  }

  public String cast_BAG_to_CHARARRAY(org.apache.pig.data.DataBag v){
    return String.valueOf(v);
  }

  public String cast_DATETIME_to_CHARARRAY(org.joda.time.DateTime v){
    return String.valueOf(v);
  }

  public String cast_TUPLE_to_CHARARRAY(org.apache.pig.data.Tuple v){
    return String.valueOf(v);
  }

  public String cast_MAP_to_CHARARRAY(java.util.Map v){
    return String.valueOf(v);
  }

  public String cast_BYTEARRAY_to_CHARARRAY(org.apache.pig.data.DataByteArray v){
    return String.valueOf(v);
  }

  public String cast_BIGINTEGER_to_CHARARRAY(java.math.BigInteger v){
    return String.valueOf(v);
  }

  public String cast_BIGDECIMAL_to_CHARARRAY(java.math.BigDecimal v){
    return String.valueOf(v);
  }

  public org.apache.pig.data.DataBag cast_BOOLEAN_to_BAG(boolean v){
    throw new IllegalArgumentException("cast BOOLEAN to BAG");
  }

  public org.apache.pig.data.DataBag cast_INT_to_BAG(int v){
    throw new IllegalArgumentException("cast INT to BAG");
  }

  public org.apache.pig.data.DataBag cast_LONG_to_BAG(long v){
    throw new IllegalArgumentException("cast LONG to BAG");
  }

  public org.apache.pig.data.DataBag cast_FLOAT_to_BAG(float v){
    throw new IllegalArgumentException("cast FLOAT to BAG");
  }

  public org.apache.pig.data.DataBag cast_DOUBLE_to_BAG(double v){
    throw new IllegalArgumentException("cast DOUBLE to BAG");
  }

  public org.apache.pig.data.DataBag cast_CHARARRAY_to_BAG(String v){
    throw new IllegalArgumentException("cast CHARARRAY to BAG");
  }

  public org.apache.pig.data.DataBag cast_BAG_to_BAG(org.apache.pig.data.DataBag v){
    return v;
  }

  public org.apache.pig.data.DataBag cast_DATETIME_to_BAG(org.joda.time.DateTime v){
    throw new IllegalArgumentException("cast DATETIME to BAG");
  }

  public org.apache.pig.data.DataBag cast_TUPLE_to_BAG(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to BAG");
  }

  public org.apache.pig.data.DataBag cast_MAP_to_BAG(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to BAG");
  }

  public org.apache.pig.data.DataBag cast_BYTEARRAY_to_BAG(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to BAG");
  }

  public org.apache.pig.data.DataBag cast_BIGINTEGER_to_BAG(java.math.BigInteger v){
    throw new IllegalArgumentException("cast BIGINTEGER to BAG");
  }

  public org.apache.pig.data.DataBag cast_BIGDECIMAL_to_BAG(java.math.BigDecimal v){
    throw new IllegalArgumentException("cast BIGDECIMAL to BAG");
  }

  public org.joda.time.DateTime cast_BOOLEAN_to_DATETIME(boolean v){
    throw new IllegalArgumentException("cast BOOLEAN to DATETIME");
  }

  public org.joda.time.DateTime cast_INT_to_DATETIME(int v){
    return new org.joda.time.DateTime((long)v);
  }

  public org.joda.time.DateTime cast_LONG_to_DATETIME(long v){
    return new org.joda.time.DateTime((long)v);
  }

  public org.joda.time.DateTime cast_FLOAT_to_DATETIME(float v){
    return new org.joda.time.DateTime((long)v);
  }

  public org.joda.time.DateTime cast_DOUBLE_to_DATETIME(double v){
    return new org.joda.time.DateTime((long)v);
  }

  public org.joda.time.DateTime cast_CHARARRAY_to_DATETIME(String v){
    return new org.joda.time.DateTime(Long.valueOf(v));
  }

  public org.joda.time.DateTime cast_BAG_to_DATETIME(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to DATETIME");
  }

  public org.joda.time.DateTime cast_DATETIME_to_DATETIME(org.joda.time.DateTime v){
    return v;
  }

  public org.joda.time.DateTime cast_TUPLE_to_DATETIME(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to DATETIME");
  }

  public org.joda.time.DateTime cast_MAP_to_DATETIME(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to DATETIME");
  }

  public org.joda.time.DateTime cast_BYTEARRAY_to_DATETIME(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to DATETIME");
  }

  public org.joda.time.DateTime cast_BIGINTEGER_to_DATETIME(java.math.BigInteger v){
    return new org.joda.time.DateTime(v.longValue());
  }

  public org.joda.time.DateTime cast_BIGDECIMAL_to_DATETIME(java.math.BigDecimal v){
    return new org.joda.time.DateTime(v.longValue());
  }

  public org.apache.pig.data.Tuple cast_BOOLEAN_to_TUPLE(boolean v){
    throw new IllegalArgumentException("cast BOOLEAN to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_INT_to_TUPLE(int v){
    throw new IllegalArgumentException("cast INT to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_LONG_to_TUPLE(long v){
    throw new IllegalArgumentException("cast LONG to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_FLOAT_to_TUPLE(float v){
    throw new IllegalArgumentException("cast FLOAT to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_DOUBLE_to_TUPLE(double v){
    throw new IllegalArgumentException("cast DOUBLE to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_CHARARRAY_to_TUPLE(String v){
    throw new IllegalArgumentException("cast CHARARRAY to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_BAG_to_TUPLE(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_DATETIME_to_TUPLE(org.joda.time.DateTime v){
    throw new IllegalArgumentException("cast DATETIME to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_TUPLE_to_TUPLE(org.apache.pig.data.Tuple v){
    return v;
  }

  public org.apache.pig.data.Tuple cast_MAP_to_TUPLE(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_BYTEARRAY_to_TUPLE(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_BIGINTEGER_to_TUPLE(java.math.BigInteger v){
    throw new IllegalArgumentException("cast BIGINTEGER to TUPLE");
  }

  public org.apache.pig.data.Tuple cast_BIGDECIMAL_to_TUPLE(java.math.BigDecimal v){
    throw new IllegalArgumentException("cast BIGDECIMAL to TUPLE");
  }

  public java.util.Map cast_BOOLEAN_to_MAP(boolean v){
    throw new IllegalArgumentException("cast BOOLEAN to MAP");
  }

  public java.util.Map cast_INT_to_MAP(int v){
    throw new IllegalArgumentException("cast INT to MAP");
  }

  public java.util.Map cast_LONG_to_MAP(long v){
    throw new IllegalArgumentException("cast LONG to MAP");
  }

  public java.util.Map cast_FLOAT_to_MAP(float v){
    throw new IllegalArgumentException("cast FLOAT to MAP");
  }

  public java.util.Map cast_DOUBLE_to_MAP(double v){
    throw new IllegalArgumentException("cast DOUBLE to MAP");
  }

  public java.util.Map cast_CHARARRAY_to_MAP(String v){
    throw new IllegalArgumentException("cast CHARARRAY to MAP");
  }

  public java.util.Map cast_BAG_to_MAP(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to MAP");
  }

  public java.util.Map cast_DATETIME_to_MAP(org.joda.time.DateTime v){
    throw new IllegalArgumentException("cast DATETIME to MAP");
  }

  public java.util.Map cast_TUPLE_to_MAP(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to MAP");
  }

  public java.util.Map cast_MAP_to_MAP(java.util.Map v){
    return v;
  }

  public java.util.Map cast_BYTEARRAY_to_MAP(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to MAP");
  }

  public java.util.Map cast_BIGINTEGER_to_MAP(java.math.BigInteger v){
    throw new IllegalArgumentException("cast BIGINTEGER to MAP");
  }

  public java.util.Map cast_BIGDECIMAL_to_MAP(java.math.BigDecimal v){
    throw new IllegalArgumentException("cast BIGDECIMAL to MAP");
  }

  public org.apache.pig.data.DataByteArray cast_BOOLEAN_to_BYTEARRAY(boolean v){
    throw new IllegalArgumentException("cast BOOLEAN to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_INT_to_BYTEARRAY(int v){
    throw new IllegalArgumentException("cast INT to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_LONG_to_BYTEARRAY(long v){
    throw new IllegalArgumentException("cast LONG to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_FLOAT_to_BYTEARRAY(float v){
    throw new IllegalArgumentException("cast FLOAT to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_DOUBLE_to_BYTEARRAY(double v){
    throw new IllegalArgumentException("cast DOUBLE to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_CHARARRAY_to_BYTEARRAY(String v){
    throw new IllegalArgumentException("cast CHARARRAY to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_BAG_to_BYTEARRAY(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_DATETIME_to_BYTEARRAY(org.joda.time.DateTime v){
    throw new IllegalArgumentException("cast DATETIME to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_TUPLE_to_BYTEARRAY(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_MAP_to_BYTEARRAY(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_BYTEARRAY_to_BYTEARRAY(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_BIGINTEGER_to_BYTEARRAY(java.math.BigInteger v){
    throw new IllegalArgumentException("cast BIGINTEGER to BYTEARRAY");
  }

  public org.apache.pig.data.DataByteArray cast_BIGDECIMAL_to_BYTEARRAY(java.math.BigDecimal v){
    throw new IllegalArgumentException("cast BIGDECIMAL to BYTEARRAY");
  }

  public java.math.BigInteger cast_BOOLEAN_to_BIGINTEGER(boolean v){
    return (v ? java.math.BigInteger.ONE : java.math.BigInteger.ZERO);
  }

  public java.math.BigInteger cast_INT_to_BIGINTEGER(int v){
    return java.math.BigInteger.valueOf((long)v);
  }

  public java.math.BigInteger cast_LONG_to_BIGINTEGER(long v){
    return java.math.BigInteger.valueOf((long)v);
  }

  public java.math.BigInteger cast_FLOAT_to_BIGINTEGER(float v){
    return java.math.BigInteger.valueOf((long)v);
  }

  public java.math.BigInteger cast_DOUBLE_to_BIGINTEGER(double v){
    return java.math.BigInteger.valueOf((long)v);
  }

  public java.math.BigInteger cast_CHARARRAY_to_BIGINTEGER(String v){
    return new java.math.BigInteger(v);
  }

  public java.math.BigInteger cast_BAG_to_BIGINTEGER(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to BIGINTEGER");
  }

  public java.math.BigInteger cast_DATETIME_to_BIGINTEGER(org.joda.time.DateTime v){
    return java.math.BigInteger.valueOf(v.getMillis());
  }

  public java.math.BigInteger cast_TUPLE_to_BIGINTEGER(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to BIGINTEGER");
  }

  public java.math.BigInteger cast_MAP_to_BIGINTEGER(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to BIGINTEGER");
  }

  public java.math.BigInteger cast_BYTEARRAY_to_BIGINTEGER(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to BIGINTEGER");
  }

  public java.math.BigInteger cast_BIGINTEGER_to_BIGINTEGER(java.math.BigInteger v){
    return v;
  }

  public java.math.BigInteger cast_BIGDECIMAL_to_BIGINTEGER(java.math.BigDecimal v){
    return java.math.BigInteger.valueOf(v.longValue());
  }

  public java.math.BigDecimal cast_BOOLEAN_to_BIGDECIMAL(boolean v){
    return (v ? java.math.BigDecimal.ONE : java.math.BigDecimal.ZERO);
  }

  public java.math.BigDecimal cast_INT_to_BIGDECIMAL(int v){
    return java.math.BigDecimal.valueOf(v);
  }

  public java.math.BigDecimal cast_LONG_to_BIGDECIMAL(long v){
    return java.math.BigDecimal.valueOf(v);
  }

  public java.math.BigDecimal cast_FLOAT_to_BIGDECIMAL(float v){
    return java.math.BigDecimal.valueOf(v);
  }

  public java.math.BigDecimal cast_DOUBLE_to_BIGDECIMAL(double v){
    return java.math.BigDecimal.valueOf(v);
  }

  public java.math.BigDecimal cast_CHARARRAY_to_BIGDECIMAL(String v){
    return new java.math.BigDecimal(v);
  }

  public java.math.BigDecimal cast_BAG_to_BIGDECIMAL(org.apache.pig.data.DataBag v){
    throw new IllegalArgumentException("cast BAG to BIGDECIMAL");
  }

  public java.math.BigDecimal cast_DATETIME_to_BIGDECIMAL(org.joda.time.DateTime v){
    return java.math.BigDecimal.valueOf(v.getMillis());
  }

  public java.math.BigDecimal cast_TUPLE_to_BIGDECIMAL(org.apache.pig.data.Tuple v){
    throw new IllegalArgumentException("cast TUPLE to BIGDECIMAL");
  }

  public java.math.BigDecimal cast_MAP_to_BIGDECIMAL(java.util.Map v){
    throw new IllegalArgumentException("cast MAP to BIGDECIMAL");
  }

  public java.math.BigDecimal cast_BYTEARRAY_to_BIGDECIMAL(org.apache.pig.data.DataByteArray v){
    throw new IllegalArgumentException("cast BYTEARRAY to BIGDECIMAL");
  }

  public java.math.BigDecimal cast_BIGINTEGER_to_BIGDECIMAL(java.math.BigInteger v){
    return java.math.BigDecimal.valueOf(v.longValue());
  }

  public java.math.BigDecimal cast_BIGDECIMAL_to_BIGDECIMAL(java.math.BigDecimal v){
    return v;
  }

  // projection operators
  public boolean proj_BOOLEAN(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (Boolean)t.get(i);
  }

  public int proj_INT(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (Integer)t.get(i);
  }

  public long proj_LONG(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (Long)t.get(i);
  }

  public float proj_FLOAT(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (Float)t.get(i);
  }

  public double proj_DOUBLE(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (Double)t.get(i);
  }

  public String proj_CHARARRAY(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (String)t.get(i);
  }

  public org.apache.pig.data.DataBag proj_BAG(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (org.apache.pig.data.DataBag)t.get(i);
  }

  public org.joda.time.DateTime proj_DATETIME(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (org.joda.time.DateTime)t.get(i);
  }

  public org.apache.pig.data.Tuple proj_TUPLE(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (org.apache.pig.data.Tuple)t.get(i);
  }

  public java.util.Map proj_MAP(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (java.util.Map)t.get(i);
  }

  public org.apache.pig.data.DataByteArray proj_BYTEARRAY(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (org.apache.pig.data.DataByteArray)t.get(i);
  }

  public java.math.BigInteger proj_BIGINTEGER(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (java.math.BigInteger)t.get(i);
  }

  public java.math.BigDecimal proj_BIGDECIMAL(org.apache.pig.data.Tuple t, int i) throws org.apache.pig.backend.executionengine.ExecException {
    return (java.math.BigDecimal)t.get(i);
  }

}
