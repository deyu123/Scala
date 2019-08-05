package com.deyu;

public class SingletonDemoEH {
    // 可见性， 对线程的可见性， 线程不能修改，但是可见， 不能保证具有原子性
    // 原子性 : 不能修改
    private  static SingletonDemoEH instance  = new SingletonDemoEH();

    public static SingletonDemoEH getInstance() {
        return instance;
    }
}
