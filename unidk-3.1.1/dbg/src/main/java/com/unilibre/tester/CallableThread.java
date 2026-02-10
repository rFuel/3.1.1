package com.unilibre.tester.tester;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;

class CallableThread implements Callable {
    @Override
    public Object call() throws Exception {
        String iam = Thread.currentThread().getName();
        ArrayList<String> list = new ArrayList<>();
        Random randObj = new Random();
        int rnd=0, tot=0;
        for (int i=0 ; i<5; i++) {
            rnd = randObj.nextInt(10);
            tot += rnd;
            Thread.sleep(500);
//            list.add(iam + "   " + String.valueOf(rnd));
        }
        list.add(iam + " Total of " + tot + " seconds");
        return list;
    }
}
