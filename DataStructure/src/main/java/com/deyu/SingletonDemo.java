package com.deyu;

import java.util.SimpleTimeZone;

public class SingletonDemo {
    // 可见性， 对线程的可见性， 线程不能修改，但是可见， 不能保证具有原子性
    // 原子性 : 不能修改
    private static  volatile  SingletonDemo instance = null;
    // 私有化构造器
    private SingletonDemo(){}

    public static SingletonDemo getInstance() {
        if (instance == null) {
            synchronized (SingletonDemo.class) {
                if (instance == null) {
                    instance = new SingletonDemo();
                }

            }
        }

        return instance;
    }


    public static void main(String[] args) {

        SingletonDemo.getInstance();
    }
}
