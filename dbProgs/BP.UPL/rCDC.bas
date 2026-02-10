      SUBROUTINE rCDC(RECORD)
      * ------------------------------------------------
      COMMON /rCDC/ DELTAS, dot, SLASH, atIM, atFM, atVM, atSV, EXTN
      * ------------------------------------------------
      *                                                *
      * This version of CDC is for Reality DB ONLY     *
      *                                                *
      * ------------------------------------------------
      IF UNASSIGNED(dot) THEN 
         OPEN 'uDELTA.LOG' TO DELTAS ELSE RETURN
         dot = '_' ; SLASH = "/" ; EXTN = ".dlog"
         atIM = "<im>" ; atFM = "<fm>"
         atVM = "<vm>" ; atSV = "<sm>"
      END
      * ------------------------------------------------
      ITEM = ACCESS(10)
      PATH = ACCESS(11)
      ACCT = ""
      FILE = ""
      IF PATH[1,1] = SLASH THEN
         ACCT = FIELD(PATH, SLASH, 2)
         FILE = FIELD(PATH, SLASH, 3)
      END ELSE
         ACCT = @ACCOUNT
         FILE = PATH
      END
      IF ACCT = "" THEN RETURN
      *
      eDate = OCONV(DATE(), "D4-")
      YY    = FIELD(eDate, "-", 3)
      MM    = FIELD(eDate, "-", 2)
      DD    = FIELD(eDate, "-", 1)
      eDate = YY:MM:DD
      eTime = OCONV(TIME(), "MTS")
      CONVERT ":" TO "" IN eTime
      LCK.CNT=0
      LOOP
         LCNT = ("000":LCK.CNT) "R#3"
         KEY = eDate:eTime:LCNT:EXTN
         HDR = ACCT:atIM:FILE:atIM:ITEM:atIM
         READU MT.REC FROM DELTAS, KEY ELSE 
            NREC = EREPLACE(RECORD, @SM , atSV)
            NREC = EREPLACE(NREC,   @VM , atVM)
            NREC = EREPLACE(NREC,   @FM , atFM)
            NREC = HDR:NREC
            WRITE NREC ON DELTAS, KEY
            RELEASE DELTAS, KEY
            EXIT
         END
         LCK.CNT+=1
      REPEAT
      RELEASE DELTAS, KEY
      RETURN
   END
