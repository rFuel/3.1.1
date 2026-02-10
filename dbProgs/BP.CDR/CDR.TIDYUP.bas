      PROMPT ""
$INCLUDE I_Prologue
      *
      SLISTS   = "&SAVEDLISTS&"
      CALL SR.FILE.OPEN (ERR, SLISTS    , SL     ) ; IF ERR # "" THEN GO END..PROG
      CALL SR.FILE.OPEN (ERR, "BP.UPL"  , BP.UPL ) ; IF ERR # "" THEN GO END..PROG
      *
      * -------------- [ Added Cache Killer for BaaS ] ---------------------------
$IFDEF isRT
      CMD = SENTENCE()
      CMD = EREPLACE(CMD, " ", @VM)
      LOCATE "CDR.TIDYUP" IN CMD<1> SETTING cPOS ELSE GO END..PROG
      CORREL = CMD<1, cPOS+1>
      IF CORREL # "" THEN 
         GOSUB REMOVE..CACHE
         GO END..PROG
      END
$ELSE
      CMD = @SENTENCE
      TXT = FIELD(TRIM(CMD), " ", 1)
      IF TXT # "PHANTOM" THEN
         * kill the cache for CorrelationId
         TXT = TRIM(FIELD(CMD, " ", 2))
         IF TXT # "" THEN
            CORREL = TXT
            GOSUB REMOVE..CACHE
            GO END..PROG
         END
      END
$ENDIF
      *
      LOOP
         READV STOP.SW FROM BP.UPL, "STOP", 1 ELSE STOP.SW=""
         IF UPCASE(STOP.SW) = "STOP" THEN EXIT
         *
         GOSUB CLEANUP
         RQM ; RQM ; RQM ; RQM ; RQM
      REPEAT
END..PROG:
      STOP
      *
CLEANUP:
      TODAY = DATE()
      NOW   = INT(TIME())
      YESTERDAY = TODAY - 1
      READU OLD.CTL FROM SL, YESTERDAY THEN
         EOL = DCOUNT(OLD.CTL<1>, @VM)
         FOR O = 1 TO EOL
            DELETE SL, OLD.CTL<1,O>
         NEXT O
         DELETE SL, YESTERDAY
      END
      RELEASE SL, YESTERDAY
      *
      READU CDR.CTL FROM SL, TODAY ELSE CDR.CTL = ""
      EOL = DCOUNT(CDR.CTL<1>, @VM)
      FOR O = 1 TO EOL
         IF NOW > CDR.CTL<2, O> THEN
            DELETE SL, CDR.CTL<1, O>
            CDR.CTL = DELETE(CDR.CTL, 1, O, 0)
            CDR.CTL = DELETE(CDR.CTL, 2, O, 0)
         END
      NEXT O
      RELEASE SL, TODAY
      RETURN
REMOVE..CACHE:
      TODAY = DATE()
      READU CDR.CTL FROM SL, TODAY ELSE CDR.CTL = ""
      LOCATE(CORREL, CDR.CTL, 1; FND) THEN
         DELETE SL, CORREL
         CDR.CTL = DELETE(CDR.CTL, 1, FND, 0)
         CDR.CTL = DELETE(CDR.CTL, 2, FND, 0)
      END
      IF CDR.CTL<1> = "" THEN 
         DELETE SL, TODAY
      END ELSE
         WRITE CDR.CTL ON SL, TODAY
      END
      RELEASE SL, TODAY
      RETURN
   END

