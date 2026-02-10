package com.unilibre.dataserver;

public class RFuelProcess {
    // user="", pid="", cpu="", mem="", bkr="", task="", que="", clazz="";

    private String user;
    private int pid;
    private double cpu;
    private double mem;
    private String bkr;
    private String task;
    private String que;
    private String clazz;

    public RFuelProcess(String user, int pid, double cpu, double mem, String bkr, String task, String que, String clazz) {
        this.user = user;
        this.pid = pid;
        this.cpu = cpu;
        this.mem = mem;
        this.bkr = bkr;
        this.task= task;
        this.que = que;
        this.clazz = clazz;
    }

    // Getters and Setters
    public String getUser() { return user; }
    public int    getPid() { return pid; }
    public double getCpu() { return cpu; }
    public double getMem() { return mem; }
    public String getBkr() { return bkr; }
    public String getTask(){ return task; }
    public String getQue() { return que; }
    public String getClazz() { return clazz; }
}