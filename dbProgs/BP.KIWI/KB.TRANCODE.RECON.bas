$INCLUDE I_Prologue
      * -----------------------------------------------------------------------
      * Usage: KB.TRANCODE.RECON FILE={account*file} PRODUCT={product,product...}
      * E.G. : KB.TRANCODE.RECON FILE=RFUELPOC2*TRAN PRODUCT=S4
      *      :
      *      : Only when FILE="RESET" Clear the TRANSATION.TRANCODE.RECON file.
      * -----------------------------------------------------------------------
      PROG = "KB.TRANCODE.RECON"
      IF MEMORY.VARS(1) = "" THEN MEMORY.VARS(1) = PROG
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.LEVEL = 3
      ERR = ""
      MSG = PROG:" starting ======================================="
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      CALL SR.FILE.OPEN(ERR,"UPL.CONTROL", UPL.CONTROL)
      CALL SR.FILE.OPEN(ERR,"BP.UPL", BP.UPL)
      CALL SR.FILE.OPEN(ERR,"VOC", VOC)
      CALL SR.FILE.OPEN(ERR,"TRANSACTION.TRANCODE.RECON", TRX.RECON)
      IF ERR THEN
         MSG = "File Open block experienced an error"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP
      END ELSE
         READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
         READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
         MAT sockPROPS = ""
         MATPARSE sockPROPS FROM PARAMS
         pAns = "" ; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = pAns
         pAns = "" ; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
      END
      PROMPT ""
      * --------------------------------------------------------------------
$IFDEF isUV
      CMD = @SENTENCE:" @ @ @ @ @"
$ENDIF
      *
$IFDEF isUD
      CMD = @SENTENCE:" @ @ @ @ @"
$ENDIF
      *
$IFDEF isRT
      CMD = SENTENCE:" @ @ @ @ @"
$ENDIF
      * --------------------------------------------------------------------
      CONVERT " " TO @FM IN CMD
      LOCATE(PROG, CMD; POS) ELSE STOP "Unknown command."
      FOR I = 1 TO POS
         CMD = DELETE(CMD, 1, 0, 0)
      NEXT I
      *
      DOTS = ".......  "
      FILE = ""
      PRD.LIST = ""
      EOI = DCOUNT(CMD, @FM)
      FOR I = 1 TO EOI
         BEGIN CASE
            CASE UPCASE(FIELD(CMD<I>, "=", 1)) = "FILE"
               FILE = FIELD(CMD<I>, "=", 2)
            CASE UPCASE(FIELD(CMD<I>, "=", 1)) = "PRODUCT"
               PRD.LIST = FIELD(CMD<I>, "=", 2)
               IF PRD.LIST = "*" THEN PRD.LIST = ""          ; * all products
         END CASE
      NEXT I
      MSG.ISO = ""
      IF FILE = "" THEN
         ERR = "No file given"
      END ELSE
         IF FILE = "RESET" THEN
            CLEARFILE TRX.RECON
            MSG = "TRANSACTION.TRANCODE.RECON has been cleared."
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            STOP
         END
         MSG.ISO = FILE:"*":PRD.LIST:" "
         IF INDEX(FILE, "*", 1) > 0 THEN
            ACCT = FIELD(FILE, "*", 1)
            FYLE = FIELD(FILE, "*", 2)
            QF = "Q":@FM:ACCT:@FM:FYLE
            FN = "upl_":ACCT:"_":FYLE
            WRITE QF ON VOC, FN
            CALL SR.FILE.OPEN(ERR, FN, IOFILE)
            IF ERR # "" THEN ERR = "The file [":FILE:"] is not in the VOC"
         END ELSE
            CALL SR.FILE.OPEN(ERR, FILE, IOFILE)
            IF ERR # "" THEN ERR = "The file [":FILE:"] is not in the VOC"
         END
      END
      IF ERR # "" THEN
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG.ISO:MSG)
         STOP
      END
      IF PRD.LIST = "" THEN
         ALL.PRODS = 1
         EOP = 0
      END ELSE
         ALL.PRODS = 0
         PRD.LIST = EREPLACE(PRD.LIST, ",", @FM)
         EOP = DCOUNT(PRD.LIST, @FM)
      END
      BCD = " BCD"
      * --------------------------------------------------------------------
      CNT1 = 0
      CNT2 = 0
      SHOWAT = 100000
      isEXT = 0
      isTARC= 0
      IF FIELD(FILE, ".", 2) = "EXT" THEN isEXT = 1
      IF FIELD(FILE, ".", 2) = "ARC" THEN isTARC = 1
      FF = 17
      IF isEXT THEN FF = 1
      IF isTARC THEN FF = 2
      CNV = "MD2"
      SELECT IOFILE
      LOOP
         READNEXT ID ELSE EXIT
         CNT1 += 1
         IF MOD(CNT1, SHOWAT)=0 THEN
            MSG = "Checked ":(DOTS:OCONV(CNT1, "MD0,")) "R#12":" records and processed ":OCONV(CNT2, "MD0,")
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG.ISO:MSG)
         END
         LAST.CHR = ID[LEN(ID),1]
         IF INDEX(BCD, LAST.CHR, 1) THEN CONTINUE
         CLID = ID
         CLID = FIELD(CLID, "S", 1)
         CLID = FIELD(CLID, "L", 1)
         CLID = FIELD(CLID, "I", 1)
         TEMP = ID[LEN(CLID)+1, LEN(ID)]
         TEMP = FIELD(TEMP, ".", 1)
         TEMP = FIELD(TEMP, "/", 1)
         THIS.PROD = TEMP
         IF NOT(ALL.PRODS) THEN
            LOCATE(THIS.PROD, PRD.LIST; FND) ELSE CONTINUE
         END
         *
         KEY = FILE:"*":THIS.PROD:"*":ID
         *
         READU TREC FROM IOFILE, ID ELSE CONTINUE
         READU OREC FROM TRX.RECON, KEY ELSE OREC = ""
         EOI = DCOUNT(TREC, @FM)
         FOR T = FF TO EOI
            TC = TREC<T,1,1>             ; * Tran Code
            TA = OCONV(TREC<T,3,1>, CNV)           ; * Tran Amount
            LOCATE TC IN OREC<1,1> BY "AR" SETTING VM THEN
               OREC<2,VM>+=1
               OREC<3,VM>+=TA
            END ELSE
               OREC = INSERT(OREC, 1, VM, 0, TC)
               OREC = INSERT(OREC, 2, VM, 0, 1)
               OREC = INSERT(OREC, 3, VM, 0, TA)
            END
         NEXT T
         WRITE OREC ON TRX.RECON, KEY
         CNT2 += 1
         RELEASE IOFILE, ID
         RELEASE TRX.RECON, KEY
      REPEAT
      MSG = "Finished ........................................................"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG.ISO:MSG)
      MSG = "Checked ":CNT1:" records and processed ":OCONV(CNT2, "MD0,")
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG.ISO:MSG)
      FN = FILEINFO(IOFILE, 1)
      IF FN[1,3] = "upl" THEN DELETE VOC, FN
      STOP
   END
