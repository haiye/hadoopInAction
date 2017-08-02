package michael.ye.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

public class MyZookeeper {

    private ZooKeeper zookeeper;

    /**
     * 
     * @param connectString
     *            以英文逗号","分隔的"host:port",每一个"host:port"都代表一个Zookeeper机器
     *            也可以指定根目录，如"host:port/chroot"，所有对Zookeeper的操作都针对该目录，实现客户端隔离命名空间
     * @param sessionTimeout
     *            回话的超时时间，以毫秒为单位 一个会话中没有进行有效的心跳检测，该会话就失效
     * @param watcher
     *            事件通知处理器，可以设置为null
     */
    public MyZookeeper(String connectString, int sessionTimeout, Watcher watcher) {
        this(connectString, sessionTimeout, watcher, false);
    }

    /**
     * 
     * @param connectString
     *            以英文逗号","分隔的"host:port",每一个"host:port"都代表一个Zookeeper机器
     *            也可以指定根目录，如"host:port/chroot"，所有对Zookeeper的操作都针对该目录，实现客户端隔离命名空间
     * @param sessionTimeout
     *            回话的超时时间，以毫秒为单位 一个会话中没有进行有效的心跳检测，该会话就失效
     * @param watcher
     *            事件通知处理器，可以设置为null
     * @param canBeReadOnly
     *            标识当前会话是否"read-only"
     */
    public MyZookeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) {
        try {
            zookeeper = new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * @param connectString
     *            以英文逗号","分隔的"host:port",每一个"host:port"都代表一个Zookeeper机器
     *            也可以指定根目录，如"host:port/chroot"，所有对Zookeeper的操作都针对该目录，实现客户端隔离命名空间
     * @param sessionTimeout
     *            回话的超时时间，以毫秒为单位 一个会话中没有进行有效的心跳检测，该会话就失效
     * @param watcher
     *            事件通知处理器，可以设置为null
     * @param sessionId
     *            sessionId和sessionPasswd能够唯一确定一个会话，客户端可以使用这两个参数恢复会话
     *            sessionId通过Zookeeper实例的getSessionId方法获得
     * @param sessionPasswd
     *            sessionId和sessionPasswd能够唯一确定一个会话，客户端可以使用这两个参数恢复会话
     *            sessionPasswd通过Zookeeper实例的getSessionPasswd方法获得
     */
    public MyZookeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId,
            byte[] sessionPasswd) {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, false);
    }

    /**
     * 
     * @param connectString
     *            以英文逗号","分隔的"host:port",每一个"host:port"都代表一个Zookeeper机器
     *            也可以指定根目录，如"host:port/chroot"，所有对Zookeeper的操作都针对该目录，实现客户端隔离命名空间
     * @param sessionTimeout
     *            回话的超时时间，以毫秒为单位 一个会话中没有进行有效的心跳检测，该会话就失效
     * @param watcher
     *            事件通知处理器，可以设置为null
     * @param sessionId
     *            sessionId和sessionPasswd能够唯一确定一个会话，客户端可以使用这两个参数恢复会话
     *            sessionId通过Zookeeper实例的getSessionId方法获得
     * @param sessionPasswd
     *            sessionId和sessionPasswd能够唯一确定一个会话，客户端可以使用这两个参数恢复会话
     *            sessionPasswd通过Zookeeper实例的getSessionPasswd方法获得
     * @param canBeReadOnly
     *            标识当前会话是否"read-only"
     */
    public MyZookeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd,
            boolean canBeReadOnly) {
        try {
            zookeeper = new ZooKeeper(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, canBeReadOnly);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个znode
     * 
     * @param path
     *            节点路径
     * @param data
     *            节点的初始内容
     * @param acl
     *            节点的ACL策略
     * @param createMode
     *            节点的类型
     * @return 若创建成功返回true，否则返回false;
     */
    public boolean createZnode(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        try {
            String createdPath = zookeeper.create(path, data, acl, createMode);
            System.out.println("response of createZnode:"+createdPath);
            if (createdPath.equals(path)) {
                return true;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 创建一个znode
     * 不支持多层创建，即父节点不存在时不能创建子节点
     * 
     * @param path
     *            节点路径
     * @param data
     *            节点的初始内容
     * @param acl
     *            节点的ACL策略
     * @param createMode
     *            节点的类型
     * @return 若创建成功返回true，否则返回false;
     */
    
    /**
     * 创建一个znode
     * 不支持多层创建，即父节点不存在时不能创建子节点
     * @param path
     *            节点路径
     * @param data
     *            节点的初始内容
     * @param acl
     *            节点的ACL策略
     * @param createMode
     *            节点的类型
     * @param cb
     *            异步回调函数，当服务器端创建节点结束后，Zookeeper客户端就自动调用这个方法
     * @param ctx
     *            用于传递对象，可以在回调方法执行的时候使用，通常是放一个上下文context信息
     * @return 若创建成功返回true，否则返回false;
     */
    public boolean createZnode(final String path, byte data[], List<ACL> acl, CreateMode createMode, StringCallback cb,
            Object ctx) {
        zookeeper.create(path, data, acl, createMode, cb, ctx);
        return true;

    }

    /**
     * 删除一个znode
     * @param path
     *          节点路径
     * @param version
     *          节点的数据版本, 标识本次删除操作针对该数据版本进行
     *          if the given version is -1, it matches any node's versions
     * @return
     */
    public boolean deleteZnode(String path, int version) {
        try {
            zookeeper.delete(path, version);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return true;
    }
    /**
     * 
     * 删除一个znode
     * @param path
     *          节点路径
     * @param version
     *          节点的数据版本, 标识本次删除操作针对该数据版本进行
     *          if the given version is -1, it matches any node's versions
     * @param cb
     *          异步回调函数，当服务器端删除节点结束后，Zookeeper客户端就自动调用这个方法
     * @param ctx
     *          用于传递对象，可以在回调方法执行的时候使用，通常是放一个上下文context信息
     */
    public void deleteZnode(final String path, int version, VoidCallback cb, Object ctx){
        zookeeper.delete(path, version, cb, ctx);    
    }
    
    public void getChildren(final String path, Watcher watcher, Children2Callback cb, Object ctx){
        zookeeper.getChildren(path, watcher, cb, ctx);
    }
    
 

    public boolean setZnode(String path, String value) {
        return true;
    }

    public  void register(Watcher watcher) {
        zookeeper.register(watcher);
    }
}
