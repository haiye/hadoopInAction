package michael.ye.zookeeper.callback;

import org.apache.zookeeper.AsyncCallback;

public class MyStringCallback implements AsyncCallback.StringCallback{


    /**
     * @param rc
     * @param expectedPath
     *          想要创建的path
     * @param ctx
     *          用于传递对象，可以再回调方法执行的时候使用，通常是放一个上下文context信息
     * @param realPath
     *          实际创建的path
     *          实际创建的path 有可能 不等于 想要创建的path，原因在于创建顺序的znode, 如创建/city/hangzhou,实际上变成了/city/hangzhou0000000002
     */
    public void processResult(int rc, String expectedPath, Object ctx, String realPath) {
        System.out.println("create path result:[rc="+rc+" expectedPath="+expectedPath+" ctx="+ctx+" realPath="+realPath);
    }

}
