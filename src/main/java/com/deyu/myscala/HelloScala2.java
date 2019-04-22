package com.deyu.myscala;

//类
public final class HelloScala2
{
    public static void main(String[] paramArrayOfString)
    {
        HelloScala2$.MODULE$.main(paramArrayOfString);
    }
}

final class HelloScala2$
{

    public static final HelloScala2$ MODULE$;

    static
    {
        MODULE$ = new HelloScala2$();
    }

    public void main(String[] args)
    {

        System.out.println("hello,scala!~~~~~~ 模拟");
    }
    //private HelloScala$() { MODULE$ = this; }

}

