package org.apache.flink.table.planner.plan.logical;

public class DistributeSpec implements WindowSpec {
    @Override
    public String toSummaryString(String windowing) {
        return "distribute";
    }
}
