package com.jwszol;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.ArrayList;

public class TransactionAnalyseJob {

    private List<Integer> wban_Number_List = new ArrayList<>();
    private List<Integer> yearMonthDay_List = new ArrayList<>();
    private List<Integer> avg_Temp_List = new ArrayList<>();
    private List<Integer> wind_Avg_Speed_List = new ArrayList<>();

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

        System.out.println("List avg_Temp before cleaning data -> " + old_avg_Temp_List);
        System.out.println("Size List avg_Temp before cleaning data -> " + old_avg_Temp_List.size());

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

        for(int i = 0; i < old_avg_Temp_List.size(); i++){
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

        System.out.println("List avg_Temp before conversion -> " + avg_Temp_List_String);
        System.out.println("Size List avg_Temp before conversion -> " + avg_Temp_List_String.size());

        for(int i = wban_Number_List_String.size()-1; i >= 0; i--){
            try{
                int wban_Number_int = Integer.valueOf(wban_Number_List_String.get(i));
                int yearMonthDay_int = Integer.valueOf(yearMonthDay_List_String.get(i));
                int avg_Temp_int = Integer.valueOf(avg_Temp_List_String.get(i));
                int wind_Avg_Speed_int = Integer.valueOf(wind_Avg_Speed_List_String.get(i));

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
            avg_Temp_List.add(Integer.valueOf(avg_Temp_List_String.get(i)));
            wind_Avg_Speed_List.add(Integer.valueOf(wind_Avg_Speed_List_String.get(i)));
        }
    }

    public void print_data(){
        System.out.println("List avg_Temp -> " + avg_Temp_List);
        System.out.println("Size List wban_Number -> " + wban_Number_List.size());
        System.out.println("Size List yearMonthDay -> " + yearMonthDay_List.size());
        System.out.println("Size List avg_Temp -> " + avg_Temp_List.size());
        System.out.println("Size List wind_Avg_Speed -> " + wind_Avg_Speed_List.size());
        System.out.println("\n\n");
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

        how_many_List.add(how_many);

        System.out.println("Size of List specific wban_Number -> " + wban_Number_List_amount_of_id.size());
        System.out.println("Specific wban_Number -> " + wban_Number_List_amount_of_id);
        System.out.println("Size of List How many times is specific wban_Number -> " + how_many_List.size());
        System.out.println("How many times is specific wban_Number -> " + how_many_List);
        System.out.println("\n\n");


        List<Integer> sum_per_month_avg_Temp_List = new ArrayList<>();
        int sum_per_month_avg_Temp = 0;
        int j = 0;
        int index_of_how_many_List = 0;
        for(int i = 0; i<avg_Temp_List.size(); i++){
            if (j+1 >= how_many_List.get(index_of_how_many_List))
            {

                index_of_how_many_List++;
                sum_per_month_avg_Temp_List.add(sum_per_month_avg_Temp);
                j = 0;
                sum_per_month_avg_Temp = 0;
                if (index_of_how_many_List == how_many_List.size()){
                    break;
                }
            }
            else{
                j++;
                sum_per_month_avg_Temp += avg_Temp_List.get(i);

            }
        }

        System.out.println("Size list sum of avr temp -> " + sum_per_month_avg_Temp_List.size());
        System.out.println("Sum of avr temp -> " + sum_per_month_avg_Temp_List);


//        JavaPairRDD<Integer, Integer> sum_per_month_avg_Temp_List =
//                rawTransactions.mapToPair(
//                        new PairFunction<String, Integer, Integer>() {
//                            @Override
//                            public Tuple2<Integer, Integer> call(String s) {
//                                Transaction transaction = new Transaction(s);
//                                return new Tuple2<>(transaction.getTransactionID(), transaction.getUserID());
//                            }
//                        });

        List<Double> avg_Temp_per_Month_counted = new ArrayList<>();
        double agv_Temp_value;
        for(int i = 0; i<sum_per_month_avg_Temp_List.size(); i++){
            agv_Temp_value = sum_per_month_avg_Temp_List.get(i)/how_many_List.get(i);
            avg_Temp_per_Month_counted.add(agv_Temp_value);
        }

        System.out.println("Size list avg_Temp_per_Month_counted -> " + avg_Temp_per_Month_counted.size());
        System.out.println("avg_Temp_per_Month_counted -> " + avg_Temp_per_Month_counted);
        System.out.println("\n\n");

        List<Integer> sum_per_month_wind_Avg_Speed_List = new ArrayList<>();
        int sum_per_month_wind_Avg_Speed = 0;
        j = 0;
        index_of_how_many_List = 0;
        for(int i = 0; i<wind_Avg_Speed_List.size(); i++){
            if (j+1 >= how_many_List.get(index_of_how_many_List))
            {

                index_of_how_many_List++;
                sum_per_month_wind_Avg_Speed_List.add(sum_per_month_wind_Avg_Speed);
                j = 0;
                sum_per_month_wind_Avg_Speed = 0;
                if (index_of_how_many_List == 312){
                    break;
                }
            }
            else{
                j++;
                sum_per_month_wind_Avg_Speed += wind_Avg_Speed_List.get(i);

            }
        }

        System.out.println("Size list sum of wind_Avg_Speed -> " + sum_per_month_wind_Avg_Speed_List.size());
        System.out.println("Sum of wind_Avg_Speed -> " + sum_per_month_wind_Avg_Speed_List);


        List<Double> wind_Avg_Speed_per_Month_counted = new ArrayList<>();
        double wind_Avg_Speed_value;
        for(int i = 0; i<sum_per_month_wind_Avg_Speed_List.size(); i++){
            wind_Avg_Speed_value = sum_per_month_wind_Avg_Speed_List.get(i)/how_many_List.get(i);
            wind_Avg_Speed_per_Month_counted.add(wind_Avg_Speed_value);
        }

        System.out.println("Size list wind_Avg_Speed_per_Month_counted -> " + wind_Avg_Speed_per_Month_counted.size());
        System.out.println("wind_Avg_Speed_per_Month_counted -> " + wind_Avg_Speed_per_Month_counted);


    }

    public void processing_data_for_specyfic_days(){

    }

}
