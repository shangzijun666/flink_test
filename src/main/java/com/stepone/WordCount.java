package com.stepone;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        DataSource<String> linesource = environment.readTextFile("input/words.txt");

        //计算
        FlatMapOperator<String, Tuple2<String, Long>> returns = linesource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }

        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //按照word执行分组
        UnsortedGrouping<Tuple2<String, Long>> group = returns.groupBy(0);

        //分组内进行聚合统一
        AggregateOperator<Tuple2<String, Long>> sum = group.sum(1);

        //结果打印
        sum.print();


    }
}
