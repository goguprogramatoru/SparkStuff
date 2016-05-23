/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gameloft.bi.sparkplay;

import com.gameloft.bi.sparkplay.BinaryIO.BinRow;
import com.gameloft.bi.sparkplay.BinaryIO.BinaryInputFormat;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;
import static scala.tools.nsc.interpreter.NamedParamCreator$class.tuple;

/**
 *
 * @author stefan
 */
public class Main {

    private static FSDataInputStream in;
    
    public static void main(String[] args) {
        try {
            //pi();
            //readBinary();
            //simpleRW();
            readBinarySpark();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public static void readBinarySpark(){
        try {
            SparkConf sparkConf = new SparkConf().setAppName("Read Binary");
            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
            sparkConf.set("fs.default.name", "hdfs://bird001.buc.gameloft.org:9000");

            JavaPairRDD<NullWritable, BinRow> rdd = jsc.hadoopFile(
                "hdfs://bird001.buc.gameloft.org:9000/test_io_007_rw", 
                BinaryInputFormat.class, 
                NullWritable.class, 
                BinRow.class
            );
            JavaRDD<BinRow> rowRdd = rdd.values();
            
            rowRdd.take(10).forEach(System.out::println);
            
            System.out.println("###########");
            System.out.println("##"+rowRdd.count());
            System.out.println("###########");
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public static void readBinary(){
        SparkConf sparkConf = new SparkConf().setAppName("Read Binary");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //sparkConf.set("fs.default.name", "hdfs://bird001.buc.gameloft.org:9000");
        
        BiConsumer<String, PortableDataStream> reader = (file, stream) -> readBinaryFile(file, stream);
        
        JavaPairRDD<String, PortableDataStream> rdd = jsc.binaryFiles("hdfs://bird001.buc.gameloft.org:9000/test_io_007_rw");
        System.out.println("###############################################");
        
        rdd.collectAsMap().forEach(reader);
        System.out.println("###############################################");
        System.out.println("count: "+rdd.collect().size());
        jsc.stop();
    }
    
    public static void readBinaryFile(String file, PortableDataStream portableStream){
        System.out.println("###### read : "+file);
        DataInputStream stream = portableStream.open();
        boolean isFirst = true;
        int counter = 0;
        int first=0; double second=0; short third=0; String forth="";
        
        while(true){
            try {
                counter++;
                
                first = stream.readInt();
                second = stream.readDouble();
                third = stream.readShort();
                byte[] msgBytes  = new byte[third];
                stream.read(msgBytes);
                forth = new String(msgBytes);
                
                if(isFirst){
                    System.out.println("### "+first+","+second+","+third+","+forth);
                    isFirst = false;
                }                
            }catch (EOFException ex) {    
                System.out.println("#### counter:"+counter);
                ex.printStackTrace();
                return;
            } catch (IOException ex) {
                ex.printStackTrace();
                return;
            } catch (Exception ex) {
                System.out.println("### "+first+","+second+","+third);
                System.out.println("#### counter:"+counter);
                ex.printStackTrace();
                return;
            }
        }
    }

    public static void pi() {
        System.out.println("Start JavaSparkPi");
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi");
        
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        int slices = 6;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map((integer) -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        jsc.stop();
    }
    
    public static void dynamicW() throws IOException{       
        Path file = new Path("/test_io_dyn_001_rw");
        int nbRows = 20_000_000;
        int i=0, bitCounter=0, blockSize=128*1024*1024;
        
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://bird001.buc.gameloft.org:9000");
        
        FileSystem fs = FileSystem.get(conf);
        long start = System.currentTimeMillis();
        try (FSDataOutputStream out = fs.create(file)) {
            while(i < nbRows){
                if(i%2==0){
                    String msg = "sixty-nine";
                    byte[] msgBytes = msg.getBytes();
                    
                    short rowLen = (short) (4+8+2+msgBytes.length);
                    int remaining = bitCounter%blockSize;
                    if(remaining < rowLen){
                        bitCounter+=remaining;
                        byte[] empty = new byte[remaining];
                        out.write(empty);
                    }else{
                    
                        bitCounter += rowLen;

                        out.writeShort(rowLen);
                        out.writeInt(69);
                        out.writeDouble(69.69);
                        out.writeShort(msgBytes.length);
                        out.write(msgBytes);
                    }
                }else{
                    String msg = "ninety-sixxx";
                    byte[] msgBytes = msg.getBytes();
                    
                    short rowLen = (short) (4+2+msgBytes.length+8+4);
                    bitCounter += rowLen;
                    
                    out.writeShort(rowLen);
                    out.writeInt(96);
                    out.writeShort(msgBytes.length);
                    out.write(msgBytes);
                    out.writeDouble(69.69);
                    out.writeInt(96);
                }
                
                i++;
            }
            
        }
        System.out.println("write: "+((System.currentTimeMillis()-start)));
    }
    
    
    public static void simpleRW() throws IOException{       
        Path file = new Path("/test_io_007_rw");
        int nbRows = 20_000_000;
        int i=0;
        
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://bird001.buc.gameloft.org:9000");
        
        FileSystem fs = FileSystem.get(conf);
        long start = System.currentTimeMillis();
        try (FSDataOutputStream out = fs.create(file)) {
            while(i < nbRows){
                String msg = "sixty-nine";
                byte[] msgBytes = msg.getBytes();
                out.writeInt(69);
                out.writeDouble(69.69);
                out.writeShort(msgBytes.length);
                out.write(msgBytes);
                i++;
            }
            
        }
        System.out.println("write: "+((System.currentTimeMillis()-start)));
    }
    
    public static void writeCsvFile() throws IOException{
        int nbRows = 10_000_000;
        int i=0;
        Path file = new Path("/test_io_csv_003_rw");
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://bird001.buc.gameloft.org:9000");
        FileSystem fs = FileSystem.get(conf);
        Random rand = new Random();
        try (FSDataOutputStream out = fs.create(file)) {
            StringBuffer buffer = new StringBuffer();
            while(i < nbRows){
                buffer.append("param").append(rand.nextInt(20)).append(",");
                buffer.append("param").append(rand.nextInt(20)).append(",");
                buffer.append("param").append(rand.nextInt(20)).append("\n");
                i++;
                
                if(i%1000 == 1){
                    out.write(buffer.toString().getBytes());
                    buffer = new StringBuffer();
                }
            }
            
            if(buffer.length() > 0){
                out.write(buffer.toString().getBytes());
            }
        }
    }

}
