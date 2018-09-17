package com.xdd.IRsystem;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class App {
    public static void main(String args[]) {
        Date date = new Date();
        long times = date.getTime();//时间戳
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowString = formatter.format(date);
        //配置es
        SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example")
                .set("es.nodes", "localhost")
                .set("es.port", "9200");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "filebeat-6.4.0-2018.09.04");
        JavaRDD<Log> rddLog = esRDD.map(x -> {
            String json = "";
            if (x._2.get("message") != null) {
                json = x._2.get("message").toString();
            }
            return new Log(json);
        });
        //过滤数据
        JavaRDD<Log> filterLog = rddLog.filter(item -> {
            if (!item.isEmpty && item.isInfoDetail()) {
                return true;
            } else {
                return false;
            }
        });
        //解析url
        JavaRDD<String> result = filterLog.map(item -> {
            return item.parseInfoUrl();
        });
        //保存数据
        result.coalesce(1, true).saveAsTextFile("infoData/" + nowString + "/");
        //处理数据
        JavaRDD<Rating> ratings = result.map(s -> {
            String[] sarray = s.split(" ");
            return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
        });
        //训练模型
        int rank = 10; //隐藏因子
        int numIterations = 10; //迭代次数
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        //评估模型
        JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(r -> new Tuple2<>(r.user(), r.product()));
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())));

        //合并预测值
        JavaRDD<Tuple2<Double, Double>> rateAndPreds = JavaPairRDD.fromJavaRDD(ratings.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))).join(predictions).values();

        //计算错误方差值
        double MSE = rateAndPreds.mapToDouble(pair -> {
            double erro = pair._1() - pair._2();
            return erro * erro;
        }).mean();

        System.out.println("Mean Squared Error =" + MSE);
        //保存模型
        model.save(jsc.sc(), "model/" + nowString + "/infoModel");
        //MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(), "target/tmp/myCollaborativeFilter");
        //预测用户
        Rating[] recommendList = model.recommendProducts(37581438, 10);
        for (Rating one : recommendList) {
            System.out.println("xdd:" + one.user() + " " + one.product() + " " + one.rating());
        }
        jsc.stop();
    }
}
