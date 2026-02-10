package com.unilibre.commons;

import java.util.Properties;

public class LicTest {

    public static void main(String[] args) {
        if (NamedCommon.BaseCamp.contains("/home/andy")) NamedCommon.BaseCamp = NamedCommon.DevCentre;

        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        System.out.println("-----------------------------------------------------------------");

        License.SetTest(true);
        boolean pass = License.IsValid();
        System.out.println("Lic isValid: "+pass);

        System.out.println("-----------------------------------------------------------------");
    }
}
