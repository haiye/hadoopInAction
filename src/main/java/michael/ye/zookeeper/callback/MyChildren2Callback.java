package michael.ye.zookeeper.callback;

import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

public class MyChildren2Callback implements AsyncCallback.Children2Callback{

    /**
     * @param rc
     * @param path
     *          想要监听的path
     * @param ctx
     *          用于传递对象，可以再回调方法执行的时候使用，通常是放一个上下文context信息
     * @param children
     * @param stat
     */
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        System.out.println("get children znode result:[rc="+rc+" path="+path+" ctx="+ctx+" childrenList="+children+ "stat="+stat+"]");

    }

}
