package org.apache.flink.table.planner.functions.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.TableCharacteristic;

import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;

import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

/**
 * SqlDistributeTableFunction.
 * Example from: Calcite MockSqlOperatorTable
 */
public class SqlDistributeTableFunction extends SqlFunction implements SqlTableFunction {

    private final Map<Integer, TableCharacteristic> tableParams =
        ImmutableMap.of(
            0,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.SET)
                .passColumnsThrough()
                .pruneIfEmpty()
                .build());

    public SqlDistributeTableFunction() {
        super("distribute", SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
                new OperandMetadataImpl(), SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    @Override
    public @Nullable TableCharacteristic tableCharacteristic(int ordinal) {
        return tableParams.get(ordinal);
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return !tableParams.containsKey(ordinal);
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return SqlDistributeTableFunction::inferRowType;
    }

    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType inputRowType = opBinding.getOperandType(0);
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      return typeFactory.builder()
          .kind(inputRowType.getStructKind())
          .addAll(inputRowType.getFieldList())
          .add("RANK_NUMBER", bigintType).nullable(true)
          .build();
    }

    /** Operand type checker for TUMBLE. */
    private static class OperandMetadataImpl implements SqlOperandMetadata {

      @Override public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return com.google.common.collect.ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.ANY),
            typeFactory.createSqlType(SqlTypeName.INTEGER));
      }

      @Override public List<String> paramNames() {
        return ImmutableList.of("DATA", "COL");
      }

      @Override public boolean checkOperandTypes(
          SqlCallBinding callBinding, boolean throwOnFailure) {
        final SqlNode operand1 = callBinding.operand(1);
        final SqlValidator validator = callBinding.getValidator();
        final RelDataType type = validator.getValidatedNodeType(operand1);
        if (!SqlTypeUtil.isIntType(type)) {
          if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          } else {
            return false;
          }
        } else {
          return true;
        }
      }

      @Override public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
      }

      @Override public String getAllowedSignatures(SqlOperator op, String opName) {
        return "TopN(TABLE table_name, BIGINT rows)";
      }

      @Override public Consistency getConsistency() {
        return Consistency.NONE;
      }

      @Override public boolean isOptional(int i) {
        return false;
      }
    }
}
