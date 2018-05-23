package pdpone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.LongStream;
/*
 *<h1>This class computes the averages with sequential method with fibonacci series</h1>
 * @author=Shantanu Kawlekar
 * */

public class SequentialFib{


    /*
     * this method initializes the data structures for this class and also evaluates the final output.
     * this method also times the execution and calculates the min,max and avg time
     *
     * */
    public void sequentialInit(List<String> strLst){
        HashMap<String,ArrayList<Integer>> t=null;
        HashMap<String,Double> output= new HashMap<>();
        SequentialFib sq= new SequentialFib();
        long[] timeInMs =new long[10];
        long min,max,avg,start,end;
        for(int i=0;i<10;i++) {
            start = System.currentTimeMillis();
            t = sq.sequentialCompute((ArrayList<String>) strLst);
            for(String key:t.keySet()){
                output.put(key,(double)(t.get(key).get(0))/t.get(key).get(1));
            }
            end = System.currentTimeMillis();
            timeInMs[i]= end-start;
        }
        Arrays.sort(timeInMs);
        min=timeInMs[0];
        max=timeInMs[timeInMs.length-1];
        avg=(LongStream.of(timeInMs).sum())/10;
        System.out.println("min: "+min+" | max: "+max+" | avg: "+avg);

        System.out.println(output.get("USC00033132"));
    }

    /*
     * this method computes the average of the temperatures per station
     * @param ArrayList<String> which are records for the year 1912
     * returns a Hashmap with String as the key and ArrayList<Integer> as the value
     * */
    public HashMap<String,ArrayList<Integer>> sequentialCompute(ArrayList<String> s){
        HashMap<String,ArrayList<Integer>> result= new HashMap<>();
        FibonacciSequence  fs =new FibonacciSequence();
        for(int i=0;i<s.size();i++){
            ArrayList<Integer> lst = new ArrayList<>();
            String[] temp = s.get(i).split(",");
            if(temp[2].equals("TMAX")){
                if(result.containsKey(temp[0])){
                    lst.add(result.get(temp[0]).get(0)+Integer.parseInt(temp[3]));
                    lst.add(result.get(temp[0]).get(1)+1);

                    // fibonacci function is called to waste time
                    fs.iterativeSequence(17);

                    result.put(temp[0], lst);
                }
                else{
                    lst.add(Integer.parseInt(temp[3]));
                    lst.add(1);
                    result.put(temp[0], lst);
                }
            }
        }
        return result;
    }
}
