package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;

import java.util.Arrays;
import java.util.List;

import brennus.eval.CallTreeExpression;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public final class CompiledExpressionOperator extends ExpressionOperator {

  private static final long serialVersionUID = 1L;
  private final CallTreeExpression e;
  private final GeneratedPigExpression compiled;

  public CompiledExpressionOperator(OperatorKey k, CallTreeExpression e,
      GeneratedPigExpression compiled) {
    super(k);
    this.e = e;
    this.compiled = compiled;
  }

  @Override
  public Tuple illustratorMarkup(Object in,
      Object out, int eqClassIndex) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void visit(PhyPlanVisitor v) throws VisitorException {
    // throw new UnsupportedOperationException("NYI");
  }

  @Override
  protected List<ExpressionOperator> getChildExpressions() {
    return Arrays.asList();
  }

  @Override
  public boolean supportsMultipleInputs() {
    return false;
  }

  @Override
  public String name() {
    return "[compiled name=" + compiled + "]";
  }

  @Override
  public Result getNextInteger() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalINT(input));
  }

  @Override
  public Result getNextLong() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalLONG(input));
  }

  @Override
  public Result getNextDouble() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalDOUBLE(input));
  }

  @Override
  public Result getNextFloat() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalFLOAT(input));
  }

  @Override
  public Result getNextDateTime()
      throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalDATETIME(input));
  }

  @Override
  public Result getNextString() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalCHARARRAY(input));
  }

  @Override
  public Result getNextDataByteArray()
      throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalBYTEARRAY(input));
  }

  @Override
  public Result getNextMap() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalMAP(input));
  }

  @Override
  public Result getNextBoolean() throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalBOOLEAN(input));
  }

  @Override
  public Result getNextBigInteger()
      throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalBIGINTEGER(input));
  }

  @Override
  public Result getNextBigDecimal()
      throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalBIGDECIMAL(input));
  }

  @Override
  public Result getNextTuple()
      throws ExecException {
    if (input == null) {
      return eop();
    }
    return result(compiled.evalTUPLE(input));
  }

  private Result result(Object r) {
    System.out.println(e + " (" + input + ") => " + r);
    input = null;
    return new Result(POStatus.STATUS_OK, r);
  }

  private Result eop() {
    System.out.println(e + " EOP");
    return new Result(POStatus.STATUS_EOP, null);
  }
}