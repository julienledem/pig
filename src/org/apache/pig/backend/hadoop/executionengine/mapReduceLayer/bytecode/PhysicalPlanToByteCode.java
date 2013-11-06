package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode;

import static brennus.eval.CallTreeExpression.lit;
import static brennus.eval.CallTreeExpression.param;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.*;
import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.ArythmeticOperator.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import brennus.asm.DynamicClassLoader;
import brennus.eval.CallTreeExpression;
import brennus.model.ExistingType;
import brennus.model.FutureType;

import org.apache.jasper.tagplugins.jstl.core.ForEach;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.BinaryOperator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.bytecode.GeneratedPigExpressionGenerator.Type;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
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
    private PhysicalOperator result;

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
//        return mp;
        try {
            PhysicalPlan compPlan = new PhysicalPlan();
            for (OperatorKey key : targetOpKeys) {
                final PhysicalOperator operator = mp.getOperator(key);
                compile(operator, mp, compPlan);
            }
            return compPlan;
        } catch (VisitorException e) {
            throw new RuntimeException(e);
        }
    }

    private void createBinaryOperator(BinaryOperator op, PhysicalOperator physicalOp) {
        CallTreeExpression right = expressions.pop();
        GeneratedPigExpressionGenerator.Type rtype = types.pop();
        CallTreeExpression left = expressions.pop();
        GeneratedPigExpressionGenerator.Type ltype = types.pop();
        expressions.push(
                new CallTreeExpression.MethodCallExpression(method(op, getType(physicalOp)),
                left, right));
        // TODO: check
        types.push(ltype);
        System.out.println(op);
    }

    public void compile(PhysicalOperator op, final PhysicalPlan plan, final PhysicalPlan compPlan) throws VisitorException {
        op.visit(
                new PhyPlanVisitor(
                        plan,
                        new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan)) {

                    @Override
                    public void visitLoad(POLoad ld)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitStore(POStore st)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitNative(PONative nat)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitFilter(POFilter fl)
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
                    public void visitPOForEach(POForEach nfe)
                            throws VisitorException {
                        // root?
                        super.visitPOForEach(nfe);

                        POForEach newFE = new POForEach(nfe.getOperatorKey(), new ArrayList<PhysicalPlan>()) {
                            @Override
                            public String toString() {
                                return "[compiled]"+super.toString();
                            }
                        };
                        newFE.setResultType(nfe.getResultType());

                        while (expressions.size() > 0) {
                            CallTreeExpression e = expressions.pop();
                            GeneratedPigExpressionGenerator.Type type = types.pop();
                            System.out.println(e);
                            // TODO: right params
                            FutureType t = e.compile(GeneratedPigExpression.class, ExistingType.INT, "evalINT", Tuple.class);
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
                            ExpressionOperator op = new ExpressionOperator(new OperatorKey("foo", 1)) {

                                @Override
                                public Tuple illustratorMarkup(Object in,
                                        Object out, int eqClassIndex) {
                                    throw new UnsupportedOperationException("NYI");
                                }

                                @Override
                                public void visit(PhyPlanVisitor v)
                                        throws VisitorException {
//                                    throw new UnsupportedOperationException("NYI");
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
                                public Result getNextInteger()
                                        throws ExecException {
                                    System.out.println("getNextInteger(" + input + ")");
                                    if (input == null) {
                                        return new Result(POStatus.STATUS_EOP, null);
                                    } else {
                                        int result2 = compiled.evalINT(input);
                                        input = null;
                                        return new Result(POStatus.STATUS_OK, result2);
                                    }
                                }

                                @Override
                                public Result getNextTuple()
                                        throws ExecException {
                                    throw new UnsupportedOperationException("NYI");
                                }
                            };
                            op.setResultType(DataType.INTEGER);

                            expPlan.add(op);
                            newFE.addInputPlan(expPlan, false);
                            compPlan.add(newFE);

                        }

                        result = newFE;
                        // TODO: do something with that
                    }

                    @Override
                    public void visitUnion(POUnion un)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitSplit(POSplit spl)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
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
                    public void visitConstant(ConstantExpression cnst)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitProject(POProject proj)
                            throws VisitorException {
                        System.out.println(proj);
                        GeneratedPigExpressionGenerator.Type type = getType(proj);
                        expressions.push(new CallTreeExpression.MethodCallExpression("proj_"+type, param("arg0"), lit(proj.getStartCol())));
                        types.push(type);
//                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitGreaterThan(GreaterThanExpr grt)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitLessThan(LessThanExpr lt)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitGTOrEqual(GTOrEqualToExpr gte)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitLTOrEqual(LTOrEqualToExpr lte)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitEqualTo(EqualToExpr eq)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitNotEqualTo(NotEqualToExpr eq)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitRegexp(PORegexp re)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitIsNull(POIsNull isNull)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitAdd(Add add)
                            throws VisitorException {
                        createBinaryOperator(PLUS, add);
                    }

                    @Override
                    public void visitSubtract(Subtract sub)
                            throws VisitorException {
                        createBinaryOperator(MINUS, sub);
                    }

                    @Override
                    public void visitMultiply(Multiply mul)
                            throws VisitorException {
                        createBinaryOperator(MUL, mul);
                    }

                    @Override
                    public void visitDivide(Divide dv)
                            throws VisitorException {
                        createBinaryOperator(DIV, dv);
                    }

                    @Override
                    public void visitMod(Mod mod)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitAnd(POAnd and)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitOr(POOr or)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitNot(PONot not)
                            throws VisitorException {
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
                    public void visitJoinPackage(
                            POJoinPackage joinPackage)
                            throws VisitorException {
                        throw new UnsupportedOperationException("NYI");
                    }

                    @Override
                    public void visitCast(POCast cast) {
                        CallTreeExpression operand = expressions.pop();
                        GeneratedPigExpressionGenerator.Type previousType = types.pop();
                        final GeneratedPigExpressionGenerator.Type newType = getType(cast);
                        expressions.push(new CallTreeExpression.MethodCallExpression("cast_"+previousType+"_TO_"+newType, operand));
                        System.out.println(cast);
                        types.push(newType);
//                        throw new UnsupportedOperationException("NYI");
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
