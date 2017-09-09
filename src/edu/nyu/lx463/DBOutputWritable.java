package edu.nyu.lx463;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by LyuXie on 3/17/17.
 */
public class DBOutputWritable implements DBWritable{

    private String starting_phrase;
    private String following_word;
    private int count;

    public DBOutputWritable(String starting_phrase, String following_word, int count) {
        this.starting_phrase = starting_phrase;
        this.following_word = following_word;
        this.count = count;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, starting_phrase);
        preparedStatement.setString(2, following_word);
        preparedStatement.setInt(3, count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        starting_phrase = resultSet.getString(1);
        following_word = resultSet.getString(2);
        count = resultSet.getInt(3);
    }


}
