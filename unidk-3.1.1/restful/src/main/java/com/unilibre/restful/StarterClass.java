package com.unilibre.restful;

// some classes DO NOT have public static void main so cannot be started from a shell script
// this process simply provide the main class and branches to the class required.

public class StarterClass {

    public static void main(String[] args) throws Exception {
        if (args[0].equals("")) return;
        switch (args[0].toLowerCase()) {
            case "keystoremanager":
                System.out.println("One moment: loading the KeystoreManager.");
                new KeystoreManager("");
                break;
            default:
                System.out.println("No class has been provided.");
        }
        System.out.println("All done. Thank you");
    }
}
