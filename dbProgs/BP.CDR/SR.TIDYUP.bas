      SUBROUTINE SR.TIDYUP (ERR, CORREL)
$INCLUDE I_Prologue
      *
      SLISTS   = "&SAVEDLISTS&"
      CALL SR.FILE.OPEN (ERR, SLISTS    , SL     ) ; IF ERR # "" THEN GO END..PROG
      *
      * -------------- [ Added Cache Killer for Biza ] ---------------------------
      IF CORREL # "" THEN GOSUB REMOVE..CACHE
      * --------------------------------------------------------------------------
      *
END..PROG:
      RETURN
      *
REMOVE..CACHE:
      TODAY = DATE()
      READU CDR.CTL FROM SL, TODAY ELSE CDR.CTL = ""
      LOCATE(CORREL, CDR.CTL, 1; FND) THEN
         DELETE SL, CORREL
         CDR.CTL = DELETE(CDR.CTL, 1, FND, 0)
         CDR.CTL = DELETE(CDR.CTL, 2, FND, 0)
      END
      NOW = INT(TIME())
      EOL = DCOUNT(CDR.CTL<1>, @VM)
      FOR O = 1 TO EOL
         IF NOW > CDR.CTL<2, O> THEN
            DELETE SL, CDR.CTL<1, O>
            CDR.CTL = DELETE(CDR.CTL, 1, O, 0)
            CDR.CTL = DELETE(CDR.CTL, 2, O, 0)
         END
      NEXT O
      IF CDR.CTL<1> = "" THEN 
         DELETE SL, TODAY
      END ELSE
         WRITE CDR.CTL ON SL, TODAY
      END
      RELEASE SL, TODAY
      RETURN
   END
