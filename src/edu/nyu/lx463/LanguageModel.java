package edu.nyu.lx463;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.*;

/**
 * Created by LyuXie on 3/17/17.
 */
public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;
        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            threshold = config.getInt("threshold", 20);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] wordsPlusCount = line.split("/t");

            if (wordsPlusCount.length != 2) {
                return;
            }

            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.parseInt(wordsPlusCount[1]);

            if (count < threshold) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]);
                sb.append(" ");
            }

            String inputKey = sb.toString().trim();
            String outputKey = words[words.length - 1] + "=" + count;

            context.write(new Text(inputKey), new Text(outputKey));

        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int topK;
        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            topK = config.getInt("topK", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //this -> <is=1000, is book=10>

            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
            for (Text value : values) {
                String cur_val = value.toString().trim();
                String word = cur_val.split("=")[0].trim();
                int count = Integer.parseInt(cur_val.split("=")[1].trim());

                if (tm.containsKey(count)) {
                    tm.get(count).add(word);
                }
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();

            for (int j = 0; iter.hasNext() && j < topK; j++) {
                int keyCount = iter.next();
                List<String> result = tm.get(keyCount);
                for (String s : result) {
                    context.write(new DBOutputWritable(key.toString(), s, keyCount), NullWritable.get());
                    j++;
                }
            }


        }
    }
}
