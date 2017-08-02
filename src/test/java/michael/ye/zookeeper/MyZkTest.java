package michael.ye.zookeeper;


import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import michael.ye.zookeeper.MyZookeeper;
import michael.ye.zookeeper.callback.MyChildren2Callback;
import michael.ye.zookeeper.callback.MyStringCallback;
import michael.ye.zookeeper.callback.MyVoidCallback;
import michael.ye.zookeeper.watcher.ZookeeperWatcher;


public class MyZkTest {
    
    public static void main(String[] args) {
        int num = (int) (Math.random() * 10000);
        int SESSION_TIMEOUT = 5000;
        Watcher watcher = new ZookeeperWatcher();
        AsyncCallback.StringCallback stringCallback = new MyStringCallback();
        AsyncCallback.VoidCallback voidCallBack = new MyVoidCallback();

        
        
        MyZookeeper myzk = new MyZookeeper("10.199.182.78", SESSION_TIMEOUT, watcher);
        System.out.println("\n------------- create znode "+"/city/hangzhou"+" with PERSISTENT_SEQUENTIAL-------------------------------");
        myzk.createZnode("/city/hangzhou", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        System.out.println("\n------------- create znode "+"/city/hangzhou"+" with PERSISTENT_SEQUENTIAL by callback-------------------------------");
        myzk.createZnode("/city/hangzhou", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL,stringCallback, "I'm call back context");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        System.out.println("\n------------- create znode "+"/city/hangzhou"+" with PERSISTENT_SEQUENTIAL-------------------------------");
        myzk.createZnode("/city/hangzhou", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        System.out.println("\n------------- create same znode "+"/city/hangzhou"+num+" with PERSISTENT by callback-------------------------------");
        System.out.println("------------- 1st time: create znode "+"/city/hangzhou"+num+"-------------------------------");
        myzk.createZnode("/city/hangzhou"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,stringCallback, "I'm call back context");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }        
        System.out.println("------------- 2nd time: create znode "+"/city/hangzhou"+num+"-------------------------------");
        myzk.createZnode("/city/hangzhou"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,stringCallback, "I'm call back context");
        // need to sleep some time for callback class work
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }      
        System.out.println("------------- 3rd time: create znode "+"/city/hangzhou"+num+"-------------------------------");
        myzk.createZnode("/city/hangzhou"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,stringCallback, "I'm call back context");
        // need to sleep some time for callback class work
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }    
        
        System.out.println("\n------------- delete znode "+"/city/hangzhou"+num+" by callback-------------------------------");
        myzk.deleteZnode("/city/hangzhou"+num,0,voidCallBack, "I'm call back context");
        // need to sleep some time for callback class work
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }      
        
        System.out.println("\n------------- : test children call back "+"/city/hangzhou"+num+"-------------------------------");

        System.out.println("------------- : create znode "+"/city/hangzhou"+num+"-------------------------------");
        myzk.createZnode("/city/hangzhou"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,stringCallback, "I'm call back context");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }  
        
        AsyncCallback.Children2Callback  childernCallback = new MyChildren2Callback();
        myzk.getChildren("/city/hangzhou"+num,watcher,childernCallback, "I'm call back");
        System.out.println("------------- : create znode "+"/city/hangzhou"+num+"/"+num+"-------------------------------");
        myzk.createZnode("/city/hangzhou"+num+"/"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
        System.out.println("now should not receiver child node changed info");
        myzk.createZnode("/city/hangzhou"+num+"/"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
        System.out.println("re-regist watcher using same instance, now should still receiver no child node changed info");
        myzk.register(watcher);
        myzk.createZnode("/city/hangzhou"+num+"/"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
        
        System.out.println("re-regist watcher using new instance, now should  receiver child node changed info again");
        myzk.register(new ZookeeperWatcher());
        myzk.createZnode("/city/hangzhou"+num+"/"+num, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
        
        System.out.println("END --------------------------------------------");

    }
}
