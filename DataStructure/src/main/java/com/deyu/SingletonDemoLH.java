package com.deyu;

public class SingletonDemoLH {

    // 创建静态
    private static SingletonDemoLH instance = null;

    // 私有化构造器
    private SingletonDemoLH() {}

    public static  SingletonDemoLH getInstance() {

        if ( instance == null) {
            return  instance = new SingletonDemoLH();
        }

        return  instance;
    }
}
