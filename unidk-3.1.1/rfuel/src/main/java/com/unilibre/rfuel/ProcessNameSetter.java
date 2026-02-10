package com.unilibre.rfuel;

public class ProcessNameSetter {
    static {
        System.loadLibrary("rfuelNameSetter");
    }

    public native void setProcessName(String name);

    public static void SetName(String name) {
        ProcessNameSetter setter = new ProcessNameSetter();
        setter.setProcessName(name);
    }
}

