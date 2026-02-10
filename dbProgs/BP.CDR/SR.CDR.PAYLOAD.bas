      SUBROUTINE SR.CDR.PAYLOAD ( ERR, DSD, atID, LOWDATE, MAT CALL.STRINGS, PAYLOAD )
$INCLUDE I_Prologue
      *
$IFDEF isRT
      DIM CALL.STRINGS(20)
$ENDIF
      LOG.KEY  = "CDR-OB":@FM
      ERR      = ""
      PAYLOAD  = ""
      IF CALL.STRINGS(1) = "" THEN
         * called for the first time for this DSD
         ERR = "No DSD has beed supplied."
         IF DSD                  = ""  THEN GO END..SRTN
         ERR = ""
         IF INF.LOGGING THEN
            LOG.MSG = "   .) building the DSD"
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         CALL SR.CDR.DSD ( ERR, DSD, MAT CALL.STRINGS )
$IFDEF isRT
         MATBUILD testARR FROM CALL.STRINGS
         IF DCOUNT(testARR, @FM) = 0 THEN GO END..SRTN
$ELSE
         IF INMAT(CALL.STRINGS) = 0  THEN GO END..SRTN
$ENDIF
         IF INF.LOGGING THEN
            LOG.MSG = "   .) finished."
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
      END
      *
      EQU A.ARR TO CALL.STRINGS(1)  ; * Attribute array
      EQU M.ARR TO CALL.STRINGS(2)  ; * Multivalue array
      EQU S.ARR TO CALL.STRINGS(3)  ; * Subvalue array
      EQU C.ARR TO CALL.STRINGS(4)  ; * Conv array
      EQU L.ARR TO CALL.STRINGS(5)  ; * Loop flag array
      EQU R.ARR TO CALL.STRINGS(6)  ; * Range flag array
      EQU T.ARR TO CALL.STRINGS(7)  ; * Tag field array
      *
      DIM REC.ARRAY(20) ; MAT REC.ARRAY = ""
      FILE.ARR = ""
      RECD.ARR = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      V.MARK   = "<vm>"
      S.MARK   = "<sm>"
      *
      ATR = 1
      STX = 1
      EOP = DCOUNT(A.ARR, @FM)
      PREFIX = ""
      FOR X = STX TO EOP
         IF A.ARR<X> = 0 THEN 
            CV = C.ARR<X>
            IF PREFIX="" THEN PREFIX = atID:MARKER 
            CHR = UPCASE(CV[1,1])
            IF INDEX("MDF", CHR, 1) AND CV#"" THEN
               DATUM = atID
               GOSUB DO..CONV
               atID = VAL
            END
            IF CHR="X" THEN
               FILE.LOADER = EREPLACE(CV, "|", @FM)
               FILE = FILE.LOADER<2>
               Fvar = FILE.LOADER<3>
               LOCATE(Fvar, FILE.ARR; FND) ELSE
                  FILE.ARR<-1> = Fvar
                  FND = DCOUNT(FILE.ARR, @FM)
                  CALL SR.FILE.OPEN ( ERR, FILE  , IOFILE ) ; IF ERR # "" THEN GO END..SRTN
                  READ REC.ARRAY(FND) FROM IOFILE, atID ELSE 
                     IF INF.LOGGING THEN
                        LOG.MSG = "   .) ":atID:" is not in file ":FILE
                        CALL uLOGGER(1, LOG.KEY:LOG.MSG)
                     END
                  END
               END
            END
            IF CHR="S" THEN 
               DATA.LOADER = EREPLACE(CV, "|", @FM)
               SRTN = DATA.LOADER<2>
               Fvar = DATA.LOADER<3>
               LOCATE(Fvar, FILE.ARR; FND) ELSE
                  FILE.ARR<-1> = Fvar
                  FND = DCOUNT(FILE.ARR, @FM)
                  CALL @SRTN ( ERR, atID, REPLY )
                  IF ERR = "" THEN
                     REC.ARRAY(FND) = REPLY
                  END ELSE
                     REC.ARRAY(FND) = ""
                  END
               END
            END
         END
      NEXT X
      * ----------------------------------------
      RECORD = REC.ARRAY(1)   ; * Primary record
      EOA = DCOUNT(RECORD, @FM)
      * ----------------------------------------
      PAYLOAD := PREFIX
      T.MARK = ""
      LAST.AV = ""
      FOR X = STX TO EOP
         AV = A.ARR<X>
         MV = M.ARR<X>
         SV = S.ARR<X>
         CV = C.ARR<X>
         DC = R.ARR<X>                 ;* Date Check field 1/0
         LP = L.ARR<X>
         TG = T.ARR<X>
         * ----------------------------------
         IF (AV:MV:SV) = "" THEN CONTINUE ; * this is a calculated-field place holder.
         IF TG="" AND AV=0 THEN CONTINUE
         IF TG="" THEN TG = FILE.ARR<1>
         LOCATE(TG, FILE.ARR; FND) THEN
            RECORD = REC.ARRAY(FND)
            EOA = DCOUNT(RECORD, @FM)
         END ELSE
            IF INF.LOGGING THEN
               ERR = "Unmapped file array (":TG:")"
               LOG.MSG = "   .) ":ERR
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
               PAYLOAD = ""
               GO END..SRTN
            END
            CONTINUE
         END
         * ----------------------------------
         IF MV = "" THEN MV = 1
         IF SV = "" THEN SV = 1
         IF AV = LAST.AV AND INDEX(LP, "A", 1) THEN
            AV+= 1
            IF AV > EOA THEN EXIT
         END
** CRT "Extracting Record<":AV:",":MV:",":SV:">   from ":FILE.ARR<FND>
         EOM = MV
         MVLOOP = 0
         IF INDEX(LP, "M", 1) THEN EOM = DCOUNT(RECORD<AV>, @VM) ; MVLOOP = 1
         STM = MV
         IF EOM=0 THEN EOM=1
         FOR MV = STM TO EOM
            EOS = SV
            SVLOOP = 0
            IF INDEX(LP, "S", 1) THEN EOS = DCOUNT(RECORD<AV, MV>, @SM) ; SVLOOP = 1
            STS = SV
            IF EOS=0 THEN EOS=1
            FOR SV = STS TO EOS
               IF AV = 0 THEN
                  DATUM = atID
               END ELSE
                  DATUM = RECORD<AV,MV,SV>
               END
               IF DC THEN IF DATUM < LOWDATE THEN CONTINUE
               CHR = UPCASE(CV[1,1])
               IF INDEX("MDF", CHR, 1) AND CV#"" THEN
                  GOSUB DO..CONV
               END ELSE
                  VAL = DATUM
               END
** IF VAL="" THEN VAL = "mt"
               IF SVLOOP AND SV < EOS THEN T.MARK = S.MARK ELSE T.MARK = ""
               PAYLOAD := T.MARK:VAL
** CRT PAYLOAD
            NEXT SV
            IF MVLOOP AND MV < EOM THEN T.MARK = V.MARK ELSE T.MARK = ""
            PAYLOAD := T.MARK
         NEXT MV
         IF X+1 < EOP THEN PAYLOAD := MARKER
      NEXT X
      * --------------------------------------------------------
END..SRTN:
      RETURN
      * --------------------------------------------------------
DO..CONV:
      *  Needs variables CV and DATUM
      CHR = UPCASE(CV[1,1])
      VAL = DATUM
      STRIP.CHR = ""
      IF CHR = "F" THEN
         CHR = CV[2,1]
         IF CHR='"' OR CHR = "'" THEN
            STRIP.CHR = CHR
            CHK1 = EREPLACE(CV, CHR, "")
            IF LEN(CV) - LEN(CHK1) # 2 THEN 
               IF INF.LOGGING THEN
                  LOG.MSG = "   .) *** error in conv (":CV:") No action taken on [":VAL:"]"
                  CALL uLOGGER(1, LOG.KEY:LOG.MSG)
               END
               RETURN
            END
            CHR = FIELD(CV, CHR, 2)
         END
         DROP.STR = "F" : STRIP.CHR : CHR : STRIP.CHR
         POS = EREPLACE(CV, DROP.STR, "")
         IF UPCASE(POS)="L" THEN POS = DCOUNT(DATUM, CHR)
         IF (NOT(NUM(POS)) OR POS="") THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) *** error in conv (":CV:") No action taken on [":VAL:"]"
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            RETURN
         END
         TMP = EREPLACE(DATUM, CHR, @FM)
         VAL = TMP<POS>
      END ELSE
         VAL = OCONV(DATUM, CV)
      END
      RETURN
      * --------------------------------------------------------
   END
