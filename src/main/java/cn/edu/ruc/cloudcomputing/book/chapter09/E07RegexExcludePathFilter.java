package cn.edu.ruc.cloudcomputing.book.chapter09;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class E07RegexExcludePathFilter implements PathFilter{

    private final String regex;
    public E07RegexExcludePathFilter(String regex){
        this.regex=regex;
    }
    @Override
    public boolean accept(Path path) {

        return !path.toString().matches(regex);
    }

}
