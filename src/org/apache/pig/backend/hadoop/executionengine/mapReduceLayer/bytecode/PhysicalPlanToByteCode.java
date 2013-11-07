package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;

import static brennus.eval.CallTreeExpression.lit;
import static brennus.eval.CallTreeExpression.param;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.method;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.ArithmeticOperator.DIV;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.ArithmeticOperator.MINUS;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.ArithmeticOperator.MUL;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.ArithmeticOperator.PLUS;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.BooleanOperator.AND;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.BooleanOperator.OR;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator.EQ;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator.GE;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator.GT;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator.LE;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator.LT;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator.NEQ;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type.BOOLEAN;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type.CHARARRAY;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type.DOUBLE;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type.FLOAT;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type.INT;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type.LONG;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import brennus.asm.DynamicClassLoader;
import brennus.eval.CallTreeExpression;
import brennus.model.ExistingType;
import brennus.model.FutureType;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.ArithmeticOperator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.BooleanOperator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.CompOperator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Divide;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GTOrEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Mod;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Multiply;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POIsNull;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONot;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POOr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORegexp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Subtract;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCombinerPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMultiQueryPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POOptimizedForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartitionRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class PhysicalPlanToByteCode {

  private DynamicClassLoader cl = new DynamicClassLoader();

  private Stack<CallTreeExpression> expressions = new Stack<CallTreeExpression>();
  private Stack<GeneratedPigExpressionGenerator.Type> types = new Stack<GeneratedPigExpressionGenerator.Type>();

  protected int compileId;

  private GeneratedPigExpressionGenerator.Type getType(PhysicalOperator op) {
    switch (op.getResultType()) {
    case DataType.INTEGER:
      return GeneratedPigExpressionGenerator.Type.INT;
    case DataType.BYTEARRAY:
    default:
      throw new UnsupportedOperationException("unknown type: " + op.getResultType());
    }
  }

  public PhysicalPlan compile(PhysicalPlan mp, List<OperatorKey> targetOpKeys) {
    try {
      for (OperatorKey key : targetOpKeys) {
        final PhysicalOperator operator = mp.getOperator(key);
        compile(operator, mp);
      }
    } catch (VisitorException e) {
      throw new RuntimeException(e);
    }
    return mp;
  }

  private List<PhysicalPlan> compileInnerPlan() {
    List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>();

    while (expressions.size() > 0) {
      final CallTreeExpression e = expressions.pop();
      GeneratedPigExpressionGenerator.Type type = types.pop();
      System.out.println(e);
      // TODO: right params
      FutureType t = e.compile(GeneratedPigExpression.class, ExistingType.existing(type.type), "eval" + type, Tuple.class);
      final GeneratedPigExpression compiled;
      cl.define(t);
      try {
        compiled = (GeneratedPigExpression)cl.loadClass(t.getName()).newInstance();
      } catch (InstantiationException e1) {
        throw new RuntimeException(e1);
      } catch (IllegalAccessException e1) {
        throw new RuntimeException(e1);
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }

      //                           TODO: attached the compiled expression tree to the ForEach
      PhysicalPlan expPlan = new PhysicalPlan();
      ExpressionOperator op = new CompiledExpressionOperator(new OperatorKey("compiled", compileId ++), e, compiled);
      op.setResultType(type.pigType);
      expPlan.add(op);
      plans.add(expPlan);
    }
    return plans;
  }

  private void createBinaryOperator(String methodName, Type returnType) {
    CallTreeExpression right = expressions.pop();
    GeneratedPigExpressionGenerator.Type rtype = types.pop();
    CallTreeExpression left = expressions.pop();
    GeneratedPigExpressionGenerator.Type ltype = types.pop();
    expressions.push(new CallTreeExpression.MethodCallExpression(methodName, left, right));
    types.push(returnType);
    System.out.println(methodName);
  }

  private void createArithmeticOperator(ArithmeticOperator op, PhysicalOperator physicalOp) {
    Type type = getType(physicalOp);
    createBinaryOperator(method(op, type), type);
  }

  private void createBooleanOperator(BooleanOperator op) {
    createBinaryOperator(method(op, BOOLEAN), BOOLEAN);
  }

  private void createComparisonOperator(CompOperator op) {
    Type type = types.peek();
    createBinaryOperator(method(op, type), BOOLEAN);
  }

  private void compile(PhysicalOperator op, final PhysicalPlan plan) throws VisitorException {
    op.visit(
        new PhyPlanVisitor(
            plan,
            new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan)) {

          // ----------------- things that contain an inner plan

          @Override
          public void visitFilter(POFilter fl) throws VisitorException {
            System.out.println("Filter");
            super.visitFilter(fl);
            List<PhysicalPlan> plans = compileInnerPlan();
            // TODO: rewire Filter
          }

          @Override
          public void visitPOForEach(POForEach nfe) throws VisitorException {
            System.out.println("ForEach");
            // root?
            super.visitPOForEach(nfe);

            List<PhysicalPlan> plans = compileInnerPlan();
            nfe.setInputPlans(plans);
          }

          // ----------------- expression operators to compile:

          @Override
          public void visitConstant(ConstantExpression cnst) {
            switch (cnst.getResultType()) {
            case DataType.INTEGER:
              expressions.push(lit((Integer)cnst.getValue()));
              types.push(INT);
              break;
            case DataType.LONG:
              expressions.push(lit((Long)cnst.getValue()));
              types.push(LONG);
              break;
            case DataType.FLOAT:
              expressions.push(lit((Float)cnst.getValue()));
              types.push(FLOAT);
              break;
            case DataType.DOUBLE:
              expressions.push(lit((Double)cnst.getValue()));
              types.push(DOUBLE);
              break;
            case DataType.CHARARRAY:
              expressions.push(lit((String)cnst.getValue()));
              types.push(CHARARRAY);
              break;
            default:
              throw new UnsupportedOperationException("unsupported type: " + cnst.getResultType());
            }
          }

          @Override
          public void visitProject(POProject proj) {
            System.out.println(proj);
            GeneratedPigExpressionGenerator.Type type = getType(proj);
            expressions.push(new CallTreeExpression.MethodCallExpression("proj_"+type, param("arg0"), lit(proj.getStartCol())));
            types.push(type);
          }

          @Override
          public void visitGreaterThan(GreaterThanExpr grt) {
            createComparisonOperator(GT);
          }

          @Override
          public void visitLessThan(LessThanExpr lt) {
            createComparisonOperator(LT);
          }

          @Override
          public void visitGTOrEqual(GTOrEqualToExpr gte) {
            createComparisonOperator(GE);
          }

          @Override
          public void visitLTOrEqual(LTOrEqualToExpr lte) {
            createComparisonOperator(LE);
          }

          @Override
          public void visitEqualTo(EqualToExpr eq) {
            createComparisonOperator(EQ);
          }

          @Override
          public void visitNotEqualTo(NotEqualToExpr eq) {
            createComparisonOperator(NEQ);
          }

          @Override
          public void visitRegexp(PORegexp re) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitIsNull(POIsNull isNull) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitAdd(Add add) {
            createArithmeticOperator(PLUS, add);
          }

          @Override
          public void visitSubtract(Subtract sub) {
            createArithmeticOperator(MINUS, sub);
          }

          @Override
          public void visitMultiply(Multiply mul) {
            createArithmeticOperator(MUL, mul);
          }

          @Override
          public void visitDivide(Divide dv) {
            createArithmeticOperator(DIV, dv);
          }

          @Override
          public void visitMod(Mod mod) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitAnd(POAnd and) {
            createBooleanOperator(AND);
          }

          @Override
          public void visitOr(POOr or) {
            createBooleanOperator(OR);
          }

          @Override
          public void visitNot(PONot not) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitBinCond(POBinCond binCond) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitNegative(PONegative negative) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitUserFunc(POUserFunc userFunc)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitComparisonFunc(
              POUserComparisonFunc compFunc)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitMapLookUp(POMapLookUp mapLookUp) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitCast(POCast cast) {
            CallTreeExpression operand = expressions.pop();
            GeneratedPigExpressionGenerator.Type previousType = types.pop();
            final GeneratedPigExpressionGenerator.Type newType = getType(cast);
            expressions.push(new CallTreeExpression.MethodCallExpression("cast_"+previousType+"_TO_"+newType, operand));
            types.push(newType);
          }

          // ----------------- other things

          @Override
          public void visitLoad(POLoad ld)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitStore(POStore st)
              throws VisitorException {
            System.out.println("Store");
            super.visitStore(st);
          }

          @Override
          public void visitNative(PONative nat)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitCollectedGroup(POCollectedGroup mg)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitLocalRearrange(POLocalRearrange lr)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitGlobalRearrange(
              POGlobalRearrange gr)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitPackage(POPackage pkg)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitCombinerPackage(
              POCombinerPackage pkg)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitMultiQueryPackage(
              POMultiQueryPackage pkg)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitUnion(POUnion un)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitSplit(POSplit spl)
              throws VisitorException {
            System.out.println("Split");
            super.visitSplit(spl);
          }

          @Override
          public void visitDemux(PODemux demux)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitCounter(POCounter poCounter)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitRank(PORank rank)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitDistinct(PODistinct distinct)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitSort(POSort sort)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitJoinPackage(
              POJoinPackage joinPackage)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitLimit(POLimit lim)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitCross(POCross cross)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitFRJoin(POFRJoin join)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitMergeJoin(POMergeJoin join)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitMergeCoGroup(
              POMergeCogroup mergeCoGrp)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitStream(POStream stream)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitSkewedJoin(POSkewedJoin sk)
              throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitPartitionRearrange(
              POPartitionRearrange pr)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitPOOptimizedForEach(
              POOptimizedForEach optimizedForEach)
                  throws VisitorException {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitPreCombinerLocalRearrange(
              POPreCombinerLocalRearrange preCombinerLocalRearrange) {
            throw new UnsupportedOperationException("NYI");
          }

          @Override
          public void visitPartialAgg(
              POPartialAgg poPartialAgg) {
            throw new UnsupportedOperationException("NYI");
          }

        });

  }

}
