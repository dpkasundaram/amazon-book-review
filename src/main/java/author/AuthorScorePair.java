package author;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AuthorScorePair implements WritableComparable {

  private final Text authorName; //natural key
//  private final Text score; // secondary key
  private final IntWritable score; // secondary key

  public AuthorScorePair() {
    this.authorName = new Text();
//    this.score = new Text();
    this.score = new IntWritable();
  }

  public Text getAuthorName() {
    return authorName;
  }

  public IntWritable getScore() {
    return score;
  }

  public void setAuthorName(String authorName) {
    this.authorName.set(authorName.getBytes());
  }

  public void setScore(int score) {
    this.score.set(score);
  }
//First compare by airline name then compare by month.
  @Override
  public int compareTo(Object o) {
    AuthorScorePair pair = (AuthorScorePair) o;
    int compareValue = this.authorName.compareTo(pair.getAuthorName());
    if (compareValue == 0) {
//      double score1 = Double.parseDouble(this.score.toString());
//      double score2 = Double.parseDouble(pair.getScore().toString());
//      if (score1 == score2) {
//        return 0;
//      }
//      else {
//        if (score1 < score2) {
//          return -1;
//        }
//        return 1;
//      }
      compareValue = this.score.compareTo(pair.getScore());
    }
    return -1*compareValue;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
      this.authorName.write(dataOutput);
      this.score.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.authorName.readFields(dataInput);
    this.score.readFields(dataInput);
  }
}
