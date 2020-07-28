package com.udemy.spark.app;

import com.google.common.collect.Iterators;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.util.Iterator;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSpark")
                .config("spark.mongodb.input.uri","mongodb://127.0.0.1/test.WorldCupCollection")
                .config("spark.mongodb.output.uri","mongodb://127.0.0.1/test.WorldCupCollection")
                .getOrCreate();
        System.setProperty("haadop.home.dir","E:\\haadop-common-2.2.0-bin-master");
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        final JavaRDD<String> Raw_Data = sc.textFile("C:\\Users\\ysahi\\OneDrive\\Masaüstü\\WorldCup\\WorldCupPlayers.csv");
        final JavaRDD<PlayersModel> playersRDD = Raw_Data.map(new Function<String, PlayersModel>() {
            @Override
            public PlayersModel call(String line) throws Exception {
                final String[] dizi = line.split(",",-1);
                return new PlayersModel(dizi[0], dizi[1], dizi[2], dizi[3], dizi[4], dizi[5], dizi[6], dizi[7], dizi[8]);

            }
        });
        final JavaRDD<PlayersModel> tur = playersRDD.filter(new Function<PlayersModel, Boolean>() {
            @Override
            public Boolean call(PlayersModel playersModel) throws Exception {
                return playersModel.getTeam().equals("TUR");
            }
        });

        //Messinin Dünya Kupasında çıktığı maç sayısı
        /*final JavaRDD<PlayersModel> messiRDD = playersRDD.filter(new Function<PlayersModel, Boolean>() {
            @Override
            public Boolean call(PlayersModel playersModel) throws Exception {
                return playersModel.getPlayerName().equals("MESSI");
            }

        });
        System.out.println("Messi Dünya Kupalarında " + messiRDD.count()+"kadar maç yapmıştır."); */
        //Tüm Futblocuların çıktığı  maç toplamı
        final JavaPairRDD<String, String> mapRDD = tur.mapToPair(new PairFunction<PlayersModel, String, String>() {

            @Override
            public Tuple2<String, String> call(PlayersModel playersModel) throws Exception {
                return new Tuple2<String, String>(playersModel.getPlayerName(), playersModel.matchID);
            }
        });
       /* mapRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> line) throws Exception {
                System.out.println(line._1+" "+line._2);
            }
        }); */
        final JavaPairRDD<String, Iterable<String>> groupPlayer = mapRDD.groupByKey();
        final JavaRDD<com.udemy.spark.app.groupPlayer> resultRDD = groupPlayer.map(new Function<Tuple2<String, Iterable<String>>, groupPlayer>() {
            @Override
            public com.udemy.spark.app.groupPlayer call(Tuple2<String, Iterable<String>> dizi) throws Exception {
                final Iterator<String> iteratorraw = dizi._2().iterator();
                int size = Iterators.size(iteratorraw);
                return new groupPlayer(dizi._1, size);



            }
        });
        final JavaRDD<Document> MongoRDD = resultRDD.map(new Function<groupPlayer, Document>() {
            @Override
            public Document call(groupPlayer groupPlayer) throws Exception {
                return Document.parse(" {PlayerName :" + "'" + groupPlayer.getPlayerName() + "'" + "," + "MatchCount:" + groupPlayer.getMatchCount());

            }
        });
        //EKRANA YAZDIRIR...
       /* resultRDD.foreach(new VoidFunction<com.udemy.spark.app.groupPlayer>() {
            @Override
            public void call(com.udemy.spark.app.groupPlayer groupPlayer) throws Exception {
                System.out.println(groupPlayer.getPlayerName()+ "  " + groupPlayer.getMatchCount());
            }
        }); */
       /* groupPlayer.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> line) throws Exception {
                System.out.println(line._1+" "+line._2);
            }
        }); */
        MongoSpark.save(MongoRDD);

    }
}
