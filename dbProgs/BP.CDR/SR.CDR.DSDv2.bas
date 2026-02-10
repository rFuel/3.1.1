      SUBROUTINE SR.CDR.DSD ( ERR, DSD, MAT CALL.STRINGS )
$INCLUDE I_Prologue
$IFDEF isRT
      DIM CALL.STRINGS(20)
$ENDIF
      IF DSD = ""  THEN GO END..SRTN
      MAT CALL.STRINGS = ""
      SAV = DSD
      DSD = EREPLACE(DSD, "<fm>", @FM)
      EQU A.ARR TO CALL.STRINGS(1)
      EQU M.ARR TO CALL.STRINGS(2)
      EQU S.ARR TO CALL.STRINGS(3)
      EQU C.ARR TO CALL.STRINGS(4)
      EQU L.ARR TO CALL.STRINGS(5)  ;* loop on AMS
      EQU R.ARR TO CALL.STRINGS(6)  ;* Date range check 'this' field
      EQU T.ARR TO CALL.STRINGS(7)  ; * tag fields.
      CMA   = ","
      LOG.KEY = "CDR-OB":@FM
      INS = 1
      EOI = DCOUNT(DSD, @FM)
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":EOI:" lines of data structure definition"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      FOR I = 1 TO EOI
         LINE  = EREPLACE(DSD<I>, CMA , @FM)
         IF LINE = "" THEN CONTINUE
         TG    = TRIM(LINE<1>)
         AV    = TRIM(LINE<2>)
         MV    = TRIM(LINE<3>)
         SV    = TRIM(LINE<4>)
         CV    = LINE<5>
         LP    = ""              ;* loop flag AMS
         DC    = 0               ;* date check flag 1/0
         IF TG # "" THEN 
            IF TG[1,1] = ">" THEN 
               DC = 1
               TG = TG[2, LEN(TG)]
            END
         END
         IF INDEX(UPCASE(AV), "N", 1) THEN 
            LP := "A"
            AV = UPCASE(AV)
            AV = EREPLACE(AV, "N", "")
            AV = EREPLACE(AV, "-", "")
            IF NOT(NUM(AV)) OR AV = "" THEN AV = 1
         END
         IF INDEX(UPCASE(MV), "N", 1) THEN 
            LP := "M"
            MV = UPCASE(MV)
            MV = EREPLACE(MV, "N", "")
            MV = EREPLACE(MV, "-", "")
            IF NOT(NUM(MV)) OR MV = "" THEN MV = 1
         END
         IF INDEX(UPCASE(SV), "N", 1) THEN 
            LP := "S"
            SV = UPCASE(SV)
            SV = EREPLACE(SV, "N", "")
            SV = EREPLACE(SV, "-", "")
            IF NOT(NUM(SV)) OR SV = "" THEN SV = 1
         END
         *
         A.ARR<INS> = AV
         M.ARR<INS> = MV
         S.ARR<INS> = SV
         C.ARR<INS> = CV
         L.ARR<INS> = LP
         R.ARR<INS> = DC
         T.ARR<INS> = TG
         INS += 1
      NEXT I
      * --------------------------------------------------------
END..SRTN:
      DSD = SAV
      RETURN
      * --------------------------------------------------------
   END
