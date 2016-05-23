/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gameloft.bi.sparkplay;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 *
 * @author stefan
 */
public class BinaryIO {
    
    private static final int ROW_LEN = 24;
    
    private static final Logger logger = Logger.getLogger(BinaryIO.class);


    public static class BinaryRecordReader implements RecordReader<NullWritable, BinRow> {       
        
        private FSDataInputStream in;
        private DataInput bufferIn;
        
        private long start;
        private long end;
        private boolean done = false;
        private byte[] buffer = new byte[ROW_LEN*100_000];
        private int bufferPos = 0;
        private int bufferLimit = buffer.length;
        
        private static final Logger loggerb = Logger.getLogger(BinaryRecordReader.class);
        

        private BinRow value = null;
        
        public BinaryRecordReader(Configuration job, FileSplit split){
            try {
                Path path = split.getPath();
                FileSystem fs = path.getFileSystem(job);
                this.in = fs.open(path, 4 * 1024 * 1024);
                this.end = split.getStart() + split.getLength();
                if (split.getStart() > in.getPos()) {
                    long startT = split.getStart();
                    if(startT%ROW_LEN != 0){
                        startT = startT + (ROW_LEN - (startT%ROW_LEN));
                    }
                    in.seek(startT);    // sync to start
                }
                this.start = in.getPos();
                done = start >= end;
                
                loggerb.info(start + "-" + end + "-" + done + "-" + path);
                System.out.println(start + "-" + end + "-" + done + "-" + path);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }



        @Override
        public float getProgress() throws IOException {
            float progress;
            if (end == start) {
                progress = 0.0f;
            } else {
                progress = Math.min(1.0f, (in.getPos() - start) / (float) (end - start));
            }

            //System.out.println("progress:"+progress);
            return progress;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public boolean next(NullWritable key, BinRow value) throws IOException {
            if (done) {
                return false;
            }
            long pos = in.getPos();
            //System.out.println(pos+">="+end);
            if (pos >= end && bufferPos == 0) {
                done = true;
                value = null;
                key=null;
            } else {
                if(bufferPos == 0) {
                    if(end-pos > buffer.length){
                        in.readFully(buffer);
                        System.out.println("buffer alloc: "+buffer.length);
                        loggerb.info("buffer alloc: "+buffer.length);
                    }else{
                        int remaining = (int)(end-pos);
                        if(start == 0 || remaining%ROW_LEN != 0){
                            remaining += (ROW_LEN-remaining%ROW_LEN);
                        }
                        
                        buffer = new byte[remaining];
                        System.out.println("buffer alloc: "+buffer.length+", remaining:"+remaining);
                        loggerb.info("buffer alloc: "+buffer.length);
                        in.readFully(buffer);
                        bufferLimit = buffer.length;
                    }
                    bufferIn = new DataInputStream(new ByteArrayInputStream(buffer));
                }

                try{
                    value.readFields(bufferIn);
                }catch(Exception ex){
                    System.out.println("len:"+buffer.length+", pos:"+bufferPos);
                    ex.printStackTrace();
                    throw ex;
                }
                bufferPos += ROW_LEN;
                if(bufferPos == bufferLimit){
                    bufferPos = 0;
                }
                //System.out.println("read: "+value.getLength());
            }
            return !done;
        }

        @Override
        public NullWritable createKey() {
            return NullWritable.get();
        }

        @Override
        public BinRow createValue() {
            return new BinRow();
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

    }

    public static class BinaryInputFormat extends FileInputFormat<NullWritable, BinRow> {

        public final static String[] HOSTS = new String[]{"bird001", "bird002.buc.gameloft.org", "bird003.buc.gameloft.org"};
        
        public BinaryInputFormat() {
            super();
        }
        
        public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
            InputSplit[] splits = super.getSplits(job, numSplits);
            
            for (InputSplit split : splits) {
                FileSplit fSplit = (FileSplit) split;
                System.out.println("## "+split);
            }
            
            return splits;
        }

        @Override
        public RecordReader<NullWritable, BinRow> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
            return new BinaryRecordReader(job, (FileSplit) split);
        }

        

        
    }

    public static class BinRow implements Writable, Serializable, WritableComparable<BinRow>{
        
        int first;
        double second;
        String third;
        

        @Override
        public void write(DataOutput out) throws IOException {
            out.write(first);
            out.writeDouble(second);
            out.writeShort(third.getBytes().length);
            out.write(third.getBytes());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readDouble();
            
            short len = in.readShort();
            byte[] strB = new byte[len];
            in.readFully(strB);
            
            third = new String(strB);
        }

        @Override
        public String toString() {
            return "BinRow{" + "first=" + first + ", second=" + second + ", third=" + third + '}';
        }
        
        

        @Override
        public int compareTo(BinRow o) {
            return 0;
        }
    }

}
