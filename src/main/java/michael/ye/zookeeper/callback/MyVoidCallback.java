package michael.ye.zookeeper.callback;

import org.apache.zookeeper.AsyncCallback;

public class MyVoidCallback implements AsyncCallback.VoidCallback{


    /**
     * @param rc
     * @param path
     *          想要删除的path
     * @param ctx
     *          用于传递对象，可以再回调方法执行的时候使用，通常是放一个上下文context信息
     */
    public void processResult(int rc, String path, Object ctx) {
        System.out.println("delete path result:[rc="+rc+" path="+path+" ctx="+ctx);

    }

}
