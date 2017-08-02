package michael.ye.zookeeper.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZookeeperWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event.toString());            

        if (event.getState() == KeeperState.SyncConnected) {
            System.out.println("Sync Connected:"+event.toString());   
            if(event.getType() == EventType.None && null== event.getPath()){
                System.out.println("event type == none");
            }else if(event.getType() == EventType.NodeChildrenChanged){

                System.out.println("child changed");
            }
            
        }        
    }

}
