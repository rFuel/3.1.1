package com.unilibre.dataserver;

public class BackgroundProcess {
    private String user, bkr, task, que;
    private int pid;
    private String clazz;
    private final String MT="";
    private double cpu;
    private double mem;

    public BackgroundProcess(String user, int pid, String clazz, double cpu, double mem) {
        this.user = user;
        this.pid = pid;
        this.cpu = cpu;
        this.mem = mem;
        this.bkr = MT;
        this.task = MT;
        this.que = MT;
        this.clazz = clazz;
    }

    // Getters and Setters
    public String getUser() { return user; }
    public int getPid() { return pid; }
    public double getCpu() { return cpu; }
    public double getMem() { return mem; }
    public String getBkr() { return bkr; }
    public String getTask() { return task; }
    public String getQue() { return que; }
    public String getClazz() { return clazz; }
}