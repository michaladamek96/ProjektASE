package com.jwszol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Map;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import scala.Tuple2;

public class TransactionAnalyseJob implements Serializable {

    private List<Integer> wban_Number_List = new ArrayList<>();
    private List<Integer> yearMonthDay_List = new ArrayList<>();
//    private List<Integer> max_Temp_List = new ArrayList<>();
//    private List<Integer> min_Temp_List = new ArrayList<>();
    private List<Double> avg_Temp_List = new ArrayList<>();
//    private List<Integer> avg_Wet_Bulb_List = new ArrayList<>();
//    private List<Integer> wind_Speed_List = new ArrayList<>();
    private List<Double> wind_Avg_Speed_List = new ArrayList<>();

    private static JavaSparkContext sc;

    public TransactionAnalyseJob(JavaSparkContext sc){
        this.sc = sc;
    }

    public void read_data(String daily){
        JavaRDD<String> rawTransactions = sc.textFile(daily);

        String wban_Number, yearMonthDay, avg_Temp, wind_Avg_Speed;
//        String max_Temp, min_Temp, avg_Wet_Bulb, wind_Speed;


        List<String> old_wban_Number_List = new ArrayList<>();
        List<String> old_yearMonthDay_List = new ArrayList<>();
//        List<String> old_max_Temp_List = new ArrayList<>();
//        List<String> old_min_Temp_List = new ArrayList<>();
        List<String> old_avg_Temp_List = new ArrayList<>();
//        List<String> old_avg_Wet_Bulb_List = new ArrayList<>();
//        List<String> old_wind_Speed_List = new ArrayList<>();
        List<String> old_wind_Avg_Speed_List = new ArrayList<>();

        for(String line: rawTransactions.collect()){
            String[] values = line.split(",");

            wban_Number = values[0];
            yearMonthDay = values[1];
//            max_Temp = values[2];
//            min_Temp = values[3];
            avg_Temp = values[4];
//            avg_Wet_Bulb = values[8];
//            wind_Speed = values[18];
            wind_Avg_Speed = values[19];

            old_wban_Number_List.add(wban_Number);
            old_yearMonthDay_List.add(yearMonthDay);
//            old_max_Temp_List.add(max_Temp);
//            old_min_Temp_List.add(min_Temp);
            old_avg_Temp_List.add(avg_Temp);
//            old_avg_Wet_Bulb_List.add(avg_Wet_Bulb);
//            old_wind_Speed_List.add(wind_Speed);
            old_wind_Avg_Speed_List.add(wind_Avg_Speed);
        }

        System.out.println(old_avg_Temp_List);
        System.out.println(old_avg_Temp_List.size());

        List<String> wban_Number_List_String = new ArrayList<>();
        List<String> yearMonthDay_List_String = new ArrayList<>();
//        List<String> max_Temp_List_String = new ArrayList<>();
//        List<String> min_Temp_List_String = new ArrayList<>();
        List<String> avg_Temp_List_String = new ArrayList<>();
//        List<String> avg_Wet_Bulb_List_String = new ArrayList<>();
//        List<String> wind_Speed_List_String = new ArrayList<>();
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

//        for(int i = 0; i < old_max_Temp_List.size(); i++){
//            String line = old_max_Temp_List.get(i);
//            line = line.replaceAll(" ", "");
//            line = line.replaceAll("\\*", "");
//            max_Temp_List_String.add(line);
//        }
//
//        for(int i = 0; i < old_min_Temp_List.size(); i++){
//            String line = old_min_Temp_List.get(i);
//            line = line.replaceAll(" ", "");
//            line = line.replaceAll("\\*", "");
//            min_Temp_List_String.add(line);
//        }

        for(int i = 0; i < old_avg_Temp_List.size(); i++){
            String line = old_avg_Temp_List.get(i);
            line = line.replaceAll(" ", "");
            line = line.replaceAll("\\*", "");
            avg_Temp_List_String.add(line);
        }

//        for(int i = 0; i < old_avg_Wet_Bulb_List.size(); i++){
//            String line = old_avg_Wet_Bulb_List.get(i);
//            line = line.replaceAll(" ", "");
//            line = line.replaceAll("\\*", "");
//            avg_Wet_Bulb_List_String.add(line);
//        }
//
//        for(int i = 0; i < old_wind_Speed_List.size(); i++){
//            String line = old_wind_Speed_List.get(i);
//            line = line.replaceAll(" ", "");
//            line = line.replaceAll("\\*", "");
//            wind_Speed_List_String.add(line);
//        }

        for(int i = 0; i < old_wind_Avg_Speed_List.size(); i++){
            String line = old_wind_Avg_Speed_List.get(i);
            line = line.replaceAll(" ", "");
            line = line.replaceAll("\\*", "");
            wind_Avg_Speed_List_String.add(line);
        }

        System.out.println(avg_Temp_List_String);
        System.out.println(avg_Temp_List_String.size());

        for(int i = wban_Number_List_String.size()-1; i >= 0; i--){
            try{
                int wban_Number_int = Integer.valueOf(wban_Number_List_String.get(i));
                int yearMonthDay_int = Integer.valueOf(yearMonthDay_List_String.get(i));
//                int max_Temp_int = Integer.valueOf(max_Temp_List_String.get(i));
//                int min_Temp_int = Integer.valueOf(min_Temp_List_String.get(i));
                double avg_Temp_int = Double.valueOf(avg_Temp_List_String.get(i));
//                int avg_Wet_Bulb_int = Integer.valueOf(avg_Wet_Bulb_List_String.get(i));
//                int wind_Speed_int = Integer.valueOf(wind_Speed_List_String.get(i));
                double wind_Avg_Speed_int = Double.valueOf(wind_Avg_Speed_List_String.get(i));

            } catch(NumberFormatException nfe){
                wban_Number_List_String.remove(i);
                yearMonthDay_List_String.remove(i);
//                max_Temp_List_String.remove(i);
//                min_Temp_List_String.remove(i);
                avg_Temp_List_String.remove(i);
//                avg_Wet_Bulb_List_String.remove(i);
//                wind_Speed_List_String.remove(i);
                wind_Avg_Speed_List_String.remove(i);
            }
        }

        for(int i = 0; i < wban_Number_List_String.size(); i++){
            wban_Number_List.add(Integer.valueOf(wban_Number_List_String.get(i)));
            yearMonthDay_List.add(Integer.valueOf(yearMonthDay_List_String.get(i)));
//            max_Temp_List.add(Integer.valueOf(max_Temp_List_String.get(i)));
//            min_Temp_List.add(Integer.valueOf(min_Temp_List_String.get(i)));
            avg_Temp_List.add(Double.valueOf(avg_Temp_List_String.get(i)));
//            avg_Wet_Bulb_List.add(Integer.valueOf(avg_Wet_Bulb_List_String.get(i)));
//            wind_Speed_List.add(Integer.valueOf(wind_Speed_List_String.get(i)));
            wind_Avg_Speed_List.add(Double.valueOf(wind_Avg_Speed_List_String.get(i)));
        }
    }

    public void print_data() {
        System.out.println(avg_Temp_List);
        System.out.println(wban_Number_List.size());
        System.out.println(yearMonthDay_List.size());
//        System.out.println(max_Temp_List.size());
//        System.out.println(min_Temp_List.size());
        System.out.println(avg_Temp_List.size());
//        System.out.println(avg_Wet_Bulb_List.size());
//        System.out.println(wind_Speed_List.size());
        System.out.println(wind_Avg_Speed_List.size());
    }

    public void processing_data_for_wban_Number(){
        int first_value = wban_Number_List.get(0);
        List<Integer> wban_Number_List_amount_of_id = new ArrayList<>();
        wban_Number_List_amount_of_id.add(first_value);

        int how_many = 0;
        List<Integer> how_many_List = new ArrayList<>();

        for(int i = 1; i<wban_Number_List.size(); i++){
            int control_value = wban_Number_List.get(i);
            if (first_value != control_value){
                first_value = control_value;
                wban_Number_List_amount_of_id.add(first_value);
                how_many_List.add(how_many);
                how_many = 0;
            }
            how_many++;
        }



        System.out.println(wban_Number_List_amount_of_id);
        System.out.println(wban_Number_List_amount_of_id.size());
        System.out.println("How many times is specific wban_Number");
        System.out.println(how_many_List);


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

        JavaPairRDD<Integer, Double> summedTemp = TempByDay.reduceByKey( new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double val1, Double val2) throws Exception{
                return val1 + val2;
            }
        });

        JavaPairRDD<Integer, Double> summedWind = WindByDay.reduceByKey( new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double val1, Double val2) throws Exception{
                return val1 + val2;
            }
        });

        //System.out.println(TempByDay);
        System.out.println(TempByDay.countByKey());


        System.out.println(summedTemp.sortByKey(true).collect());
        System.out.println(summedWind.sortByKey(true).collect());

        //JavaPairRDD<Integer, Integer> avrTem = byDay.mapValues(Function)


    }

}
