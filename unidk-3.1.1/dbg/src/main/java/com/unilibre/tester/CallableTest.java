package com.unilibre.tester.tester;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class CallableTest {

    public static void main(String[] args) throws InterruptedException {
        int nbrThreads = 4;
        FutureTask[] futures = new FutureTask[nbrThreads];
        for (int i=0 ; i < nbrThreads ; i++) {
            Callable callme = new CallableThread();
            futures[i] = new FutureTask(callme);
            Thread t = new Thread(futures[i]);
            System.out.println("fire-off thread # "+i);
            t.start();
        }

        ArrayList<Integer> skip = new ArrayList<>();
        ArrayList<String>  list = new ArrayList<>();
        skip.add(999);
        boolean allDone = false;
        while (!allDone) {
            for (int i = 0; i < nbrThreads; i++) {
                if (skip.indexOf(i) > 0) continue;
                if (futures[i].isDone()) {
                    try {
                        Object o = futures[i].get();
                        if (o instanceof ArrayList) {
                            list.addAll((Collection<? extends String>) o);
                        } else {
                            System.out.println("Thread " + i + " finished: ***************************");
                        }
                        skip.add(i);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            if (skip.size() > nbrThreads) allDone = true;
        }
        if (list.size() >0) {
            int eol = list.size();
            for (int i=0 ; i<eol ; i++) {
                System.out.println(i + "  " + list.get(i));
            }
        }
        System.out.println("All done");
        System.exit(1);
    }
}