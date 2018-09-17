package com.jwszol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;

public class TransactionAnalyseJob implements Serializable {

    private List<Integer> wban_Number_List = new ArrayList<>();
    private List<Integer> yearMonthDay_List = new ArrayList<>();
    private List<Double> avg_Temp_List = new ArrayList<>();
    private List<Double> wind_Avg_Speed_List = new ArrayList<>();

    private static JavaSparkContext sc;

    public TransactionAnalyseJob(JavaSparkContext sc){
        this.sc = sc;
    }

    public void read_data(String daily){
        JavaRDD<String> rawTransactions = sc.textFile(daily);

        String wban_Number, yearMonthDay, avg_Temp, wind_Avg_Speed;


        List<String> old_wban_Number_List = new ArrayList<>();
        List<String> old_yearMonthDay_List = new ArrayList<>();
        List<String> old_avg_Temp_List = new ArrayList<>();
        List<String> old_wind_Avg_Speed_List = new ArrayList<>();

        for(String line: rawTransactions.collect()){
            String[] values = line.split(",");

            wban_Number = values[0];
            yearMonthDay = values[1];
            avg_Temp = values[4];
            wind_Avg_Speed = values[19];

            old_wban_Number_List.add(wban_Number);
            old_yearMonthDay_List.add(yearMonthDay);
            old_avg_Temp_List.add(avg_Temp);
            old_wind_Avg_Speed_List.add(wind_Avg_Speed);
        }

        //System.out.println(old_avg_Temp_List);
        //System.out.println(old_avg_Temp_List.size());

        List<String> wban_Number_List_String = new ArrayList<>();
        List<String> yearMonthDay_List_String = new ArrayList<>();
        List<String> avg_Temp_List_String = new ArrayList<>();
        List<String> wind_Avg_Speed_List_String = new ArrayList<>();

        for(int i = 0; i < old_wban_Number_List.size(); i++){
            String line = old_wban_Number_List.get(i);
            line = line.replaceAll(" ", "");
            line = line.replaceAll("\\*", "");
            wban_Number_List_String.add(line);
        }

        for(int i = 0; i < old_yearMonthDay_List.size(); i++){
            String line = old_yearMonthDay_List.get(i);
            line = line.replaceAll(" ", "");
            line = line.replaceAll("\\*", "");
            yearMonthDay_List_String.add(line);
        }

        for(int i = 0; i < old_avg_Temp_List.size(); i++) {
            String line = old_avg_Temp_List.get(i);
            line = line.replaceAll(" ", "");
            line = line.replaceAll("\\*", "");
            avg_Temp_List_String.add(line);
        }

        for(int i = 0; i < old_wind_Avg_Speed_List.size(); i++){
            String line = old_wind_Avg_Speed_List.get(i);
            line = line.replaceAll(" ", "");
            line = line.replaceAll("\\*", "");
            wind_Avg_Speed_List_String.add(line);
        }

        //System.out.println(avg_Temp_List_String);
        //System.out.println(avg_Temp_List_String.size());

        for(int i = wban_Number_List_String.size()-1; i >= 0; i--){
            try{
                int wban_Number_int = Integer.valueOf(wban_Number_List_String.get(i));
                int yearMonthDay_int = Integer.valueOf(yearMonthDay_List_String.get(i));
                double avg_Temp_int = Double.valueOf(avg_Temp_List_String.get(i));
                double wind_Avg_Speed_int = Double.valueOf(wind_Avg_Speed_List_String.get(i));

            } catch(NumberFormatException nfe){
                wban_Number_List_String.remove(i);
                yearMonthDay_List_String.remove(i);
                avg_Temp_List_String.remove(i);
                wind_Avg_Speed_List_String.remove(i);
            }
        }

        for(int i = 0; i < wban_Number_List_String.size(); i++){
            wban_Number_List.add(Integer.valueOf(wban_Number_List_String.get(i)));
            yearMonthDay_List.add(Integer.valueOf(yearMonthDay_List_String.get(i)));
            avg_Temp_List.add(Double.valueOf(avg_Temp_List_String.get(i)));
            wind_Avg_Speed_List.add(Double.valueOf(wind_Avg_Speed_List_String.get(i)));
        }
    }

//    public void print_data(){
//        System.out.println(avg_Temp_List);
//        System.out.println(wban_Number_List.size());
//        System.out.println(yearMonthDay_List.size());
//        System.out.println(avg_Temp_List.size());
//        System.out.println(wind_Avg_Speed_List.size());
//    }

    private static PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>,Integer,Double> getAverageByKey = (tuple) -> {
        Tuple2<Double, Integer> val = tuple._2;
        double total = val._1;
        int count = val._2;
        Tuple2<Integer, Double> averagePair = new Tuple2<Integer, Double>(tuple._1, total / count);
        return averagePair;
    };

    public void processing_data_for_wban_Number(){
        List<Tuple2<Integer, Double>> WbanTemp = new ArrayList<>();
        List<Tuple2<Integer, Double>> WbanWind = new ArrayList<>();

        for(int i=0; i<yearMonthDay_List.size(); i++) {
            int wban = wban_Number_List.get(i);
            double tem = avg_Temp_List.get(i);
            double wind = wind_Avg_Speed_List.get(i);

            WbanTemp.add(new Tuple2<>(wban, tem));
            WbanWind.add(new Tuple2<>(wban, wind));
        }

        JavaPairRDD<Integer, Double> TempByDay = sc.parallelizePairs(WbanTemp);
        JavaPairRDD<Integer, Double> WindByDay = sc.parallelizePairs(WbanWind);

        JavaPairRDD<Integer, Tuple2<Double, Integer>> countTemp = TempByDay.mapValues(value -> new Tuple2<Double, Integer>(value,1));
        JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedTemp = countTemp.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Integer, Double> averageTemp = reducedTemp.mapToPair(getAverageByKey);

        JavaPairRDD<Integer, Tuple2<Double, Integer>> countWind = WindByDay.mapValues(value -> new Tuple2<Double, Integer>(value,1));
        JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedWind = countWind.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Integer, Double> averageWind = reducedWind.mapToPair(getAverageByKey);

        System.out.println("Average temperature each WBan: ");
        for(int i=0; i<averageTemp.count(); i++)
            System.out.println(averageTemp.sortByKey(true).collect().get(i));

        System.out.println("#####");

        System.out.println("Average speed of wind each WBan: ");
        for(int i=0; i<averageWind.count(); i++)
            System.out.println(averageWind.sortByKey(true).collect().get(i));

        System.out.println("#####");

    }

    public void processing_data_for_specyfic_days(){
        List<Tuple2<Integer, Double>> DaysTemp = new ArrayList<>();
        List<Tuple2<Integer, Double>> DaysWind = new ArrayList<>();

        for(int i=0; i<yearMonthDay_List.size(); i++) {
            int day = yearMonthDay_List.get(i);
            double tem = avg_Temp_List.get(i);
            double wind = wind_Avg_Speed_List.get(i);

            DaysTemp.add(new Tuple2<>(day, tem));
            DaysWind.add(new Tuple2<>(day, wind));
        }

        JavaPairRDD<Integer, Double> TempByDay = sc.parallelizePairs(DaysTemp);
        JavaPairRDD<Integer, Double> WindByDay = sc.parallelizePairs(DaysWind);

        JavaPairRDD<Integer, Tuple2<Double, Integer>> countTemp = TempByDay.mapValues(value -> new Tuple2<Double, Integer>(value,1));
        JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedTemp = countTemp.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Integer, Double> averageTemp = reducedTemp.mapToPair(getAverageByKey);

        JavaPairRDD<Integer, Tuple2<Double, Integer>> countWind = WindByDay.mapValues(value -> new Tuple2<Double, Integer>(value,1));
        JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedWind = countWind.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Integer, Double> averageWind = reducedWind.mapToPair(getAverageByKey);

        System.out.println("Average temperature each day: ");
        for(int i=0; i<averageTemp.count(); i++)
            System.out.println(averageTemp.sortByKey(true).collect().get(i));

        System.out.println("#####");

        System.out.println("Average speed of wind each day: ");
        for(int i=0; i<averageWind.count(); i++)
            System.out.println(averageWind.sortByKey(true).collect().get(i));



    }

    public void processing_data_all()
    {
        //List<Tuple<Integer>> TempList = new ArrayList<>();
        //List<Tuple<Integer> WindList = new ArrayList<>();

//        for(int i = 0; i<avg_Temp_List.size(); i++)
//        {
//            double tem = avg_Temp_List.get(i);
//            double wind = wind_Avg_Speed_List.get(i);
//
//            TempList.add(new Tuple2<Integer>(tem));
//            WindList.add(new Tuple2<Integer>(wind));
//        }

        JavaRDD<Double> TempAVG = sc.parallelize(avg_Temp_List);
        JavaRDD<Double> WindAVG = sc.parallelize(wind_Avg_Speed_List);

        long number1 = TempAVG.count();
        long number2 = WindAVG.count();

        double sum1 = TempAVG.reduce((a,b)->a+b);
        double sum2 = WindAVG.reduce((a,b)->a+b);

        double avgTemp = sum1/number1;
        double avgWind = sum2/number2;

        System.out.println("Total average temperature: " + avgTemp);
        System.out.println("Total average wind speed: " + avgWind);
    }

}
