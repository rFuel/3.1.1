      SUBROUTINE SR.GETCUSTOMERID (ERR, DSD, atID, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      ERR      = ""
      PAYLOAD  = ""
      MARKER   = "<tm>"
      LOG.KEY  = "CDR-OB":@FM
      SMS_DeviceID   = ""
      TOKEN_DeviceID = ""
      hasAccounts = ""
      AGE = 0
      *
      IF INDEX(atID, ":", 1) THEN
         atID = FIELD(atID, ":", 1)
      END
      *
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.GETCUSTOMERID for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      ERR = "(GETCUSTOMERID) Bad or Missing Parameters"
      IF DSD         = "" THEN GO END..SRTN
      IF atID        = "" THEN GO END..SRTN
      IF CORREL      = "" THEN GO END..SRTN
      *
      CALL SR.FILE.OPEN (ERR, "RBI.USER", RBI.USER)                     ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.SMSOTP.USER", RBI.SMSOTP.USER)       ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.SMSOTP.CLIENT", RBI.SMSOTP.CLIENT)   ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.SMSOTP", RBI.SMSOTP)                 ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.TOKEN.USER", RBI.TOKEN.USER)         ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.TOKEN.CLIENT", RBI.TOKEN.CLIENT)     ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.TOKEN", RBI.TOKEN)                   ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "CLIENT", CLIENT)                         ; IF ERR # "" THEN GO END..SRTN
      *
      READ USER.REC FROM RBI.USER, UPCASE(atID) ELSE
         ERR = atID:" is invalid: user does not exist."
         GO END..SRTN
      END
      *
      CLID = USER.REC<1>
      PRIMARY  = CLID
      SECONDARY= USER.REC<4>
      READ CLIENT.REC FROM CLIENT, CLID ELSE
         ERR = atID:" does not have a valid customer number."
         GO END..SRTN
      END
      *
      IF INF.LOGGING THEN
         LOG.MSG = "SMS_DeviceID  ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      FILE1 = RBI.SMSOTP.USER
      FILE2 = RBI.SMSOTP.CLIENT
      FILE3 = RBI.SMSOTP
      CHKVL = 1
      GOSUB REGISTRATION..STATUS
      SMS_DeviceID   = REG.DEVICE
      *
      IF INF.LOGGING THEN
         LOG.MSG = "TOKEN_DeviceID  ----------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      FILE1 = RBI.TOKEN.USER
      FILE2 = RBI.TOKEN.CLIENT
      FILE3 = RBI.TOKEN
      CHKVL = 5
      GOSUB REGISTRATION..STATUS
      TOKEN_DeviceID   = REG.DEVICE
      *
      VERIFY.ID = atID:":":CLID
      CALL SR.CDR.VERIFY.ACCTS ( ERR, VERIFY.ID, PROC.LIST )
      IF ERR # "" THEN GO END..SRTN
      IF PROC.LIST = "" THEN hasAccounts="false" ELSE hasAccounts="true"
      *
      DOB = CLIENT.REC<16,1,1>
      IF DOB = "" THEN
         AGE = "false"
      END ELSE
         AGE = INT((DATE() - DOB) / 365)
         IF AGE > 18 THEN AGE = "true"
      END
      *
      *
      PAYLOAD = PRIMARY:MARKER:SECONDARY:MARKER:AGE:MARKER:hasAccounts:MARKER:SMS_DeviceID:MARKER:TOKEN_DeviceID:MARKER
      * --------------------------------------------------------
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      IF INF.LOGGING THEN
         LOG.MSG = "Finished processing on ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   ."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN      ; * to calling program
      * --------------------------------------------------------
REGISTRATION..STATUS:
      REG.DEVICE = ""
      READ LIST1 FROM FILE1, atID      ELSE LIST1 = ""
      READ LIST2 FROM FILE2, PRIMARY   ELSE LIST2 = ""
      EOIa = DCOUNT(LIST1, @FM)
      EOIb = DCOUNT(LIST2, @FM)
      IF EOIa => EOIb THEN EOI = EOIa ELSE EOI = EOIb
      IF INF.LOGGING THEN
$IFDEF isRT
         LOG.MSG = ""
$ELSE
         LOG.MSG = FILEINFO(FILE1,1):"  ":atID:"   ":EOIa
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = FILEINFO(FILE2,1):"  ":PRIMARY:"   ":EOIb
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
$ENDIF
      END
      USE.LIST = ""
      FOR I = 1 TO EOI
         ITEM = LIST1<I>
         LOCATE(ITEM, LIST2; FND) THEN USE.LIST<-1> = ITEM
      NEXT I
      EOI = DCOUNT(USE.LIST, @FM)
      FOR I = 1 TO EOI
         READ CHKREC FROM FILE3, USE.LIST<I> ELSE CONTINUE
         IF (CHKREC<4> = CHKVL) THEN
            REG.DEVICE = CHKREC<7,1,1>
            EXIT
         END
      NEXT I
      RETURN
   END

