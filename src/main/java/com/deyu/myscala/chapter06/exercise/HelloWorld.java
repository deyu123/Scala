package com.deyu.myscala.chapter06.exercise;

public class HelloWorld {
    public static void main(String[] args) throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        System.out.println("hello java");
        System.out.println("abc\ru");
        Double a = 2.0;
        Double b = 1.8 ;
        System.out.println(a- b);

//        Thread.sleep(3000);

        System.out.println("sleep 3000");

        String s = " a b ";
        s.trim();
        System.out.println(s);
        System.out.println(s.trim());

        System.out.println("-----------------");

//        Class<? extends String> aClass = s.getClass();
//
//        Field value = aClass.getDeclaredField("value");
//        value.setAccessible(true);
//        value.setChar("value", 'c');
//        char[] o = (char[]) value.get(s);
//
//        System.out.println(o);


//        System.out.format("%"+(2)*2+"s","");

        System.out.format("%4d",4);


    }
}
