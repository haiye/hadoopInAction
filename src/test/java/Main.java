import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;


public class Main {
public static void main(String[] args) {
    
    FileSystem a;
	String line="hello world this is shanghai\n";
    StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
    System.out.println("count="+tokenizerArticle.countTokens());

    String next_str = tokenizerArticle.nextToken();

    StringTokenizer tokenizerLine = new StringTokenizer(next_str);
    String nameStr = tokenizerLine.nextToken(); // 学生姓名部分
//    
    if(tokenizerLine.hasMoreTokens()){
    	String scoreStr = tokenizerLine.nextToken();// 成绩部分
      System.out.println("student=" + nameStr + "; score=" + scoreStr);
    }
    System.out.println("student=" + nameStr );



}
}
