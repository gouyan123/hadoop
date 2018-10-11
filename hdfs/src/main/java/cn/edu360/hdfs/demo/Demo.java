package cn.edu360.hdfs.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Demo {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(9);
        list.add(5);
        List<Integer> temp = list.stream().sorted((m,n) -> (n.compareTo(m))).collect(Collectors.toList());
        for (Integer i : temp){
            System.out.println(i);
        }
    }
}
