      SUBROUTINE uFETCH_KEYS(ANS, SERIAL, SEL, NSEL)
$INCLUDE I_Prologue
      *
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      IF DBT#"UV" AND DBT#"UD" THEN
         UPL.LOGGING = 0
         INF.LOGGING = 0
         MAT sockPROPS = ""
         CALL SR.OPEN.CREATE(ERR, "BP.UPL", "19", BP.UPL)
         IF ERR THEN
            DBT = "UV"
         END ELSE
            READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
            READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
            MATPARSE sockPROPS FROM PARAMS
            pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = pAns
            pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
            IF PARAMS="" THEN
               PARAMS<1> = UPL.LOGGING
               PARAMS<2> = INF.LOGGING
            END
            WRITE DBT ON BP.UPL, "DBT"
            WRITE PARAMS ON BP.UPL, "properties"
         END
      END
      *
      CRLF = CHAR(13):CHAR(10)
      LOG.LEVEL = 0
      IF ANS # "" THEN
         FILE = ANS
         ANS = ""
      END ELSE
         FILE = FIELD(SEL,' ', 2)
      END
      CONVERT " " TO "_" IN FILE
      EXE = SEL
      IF NSEL # "" THEN EXE := @FM:NSEL
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:"*************[ uFETCH_KEYS ] ********************")
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" Execute ":EXE)
      EXECUTE EXE CAPTURING JUNK
      NBR.ITEMS=0; MKR=""; SKIP=0
      LOOP
         READNEXT KEY ELSE EXIT
         IF KEY="" THEN SKIP+=1; CONTINUE
         ANS := MKR:KEY; MKR = CRLF
         NBR.ITEMS += 1
      REPEAT
      IF SKIP > 0 THEN CALL uLOGGER(LOG.LEVEL, LOG.KEY:" Skipped ":SKIP:" null items from ":EXE)
      IF DBT = "UV" THEN
         OPEN "&SAVEDLISTS&" TO SLISTS ELSE
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:" Cannot open &SAVEDLISTS&")
            RETURN
         END
      END ELSE
         OPEN "SAVEDLISTS" TO SLISTS ELSE
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:" Cannot open SAVEDLISTS")
            RETURN
         END
      END
      WRITE ANS ON SLISTS, "FetchKey_":SERIAL:"_":FILE
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" ":NBR.ITEMS:" records selected")
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" ":NBR.ITEMS:" records saved to FetchKey_":SERIAL:"_":FILE)
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:"****************************************************")
      **
      ** return these variables to Java
      **
      ANS = "FetchKey_":SERIAL:"_":FILE
      SERIAL = NBR.ITEMS
      RETURN
   END
