$INCLUDE I_Prologue
      * -----------------------------------------------------------------------
      * Usage: KB.TRAN.RECON FILE={account*file} PRODUCT={product,product...}  
      *      :                                                                 
      *      : Only when FILE="RESET" Clear the TRANSATION.RECON file.         
      * -----------------------------------------------------------------------
      PROG = "KB.TRAN.RECON"
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = PROG
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.LEVEL = 3
      ERR = ""
      MSG = PROG:" starting ======================================="
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      CALL SR.FILE.OPEN(ERR,"UPL.CONTROL", UPL.CONTROL)
      CALL SR.FILE.OPEN(ERR,"BP.UPL", BP.UPL)
      CALL SR.FILE.OPEN(ERR,"VOC", VOC)
      CALL SR.FILE.OPEN(ERR,"TRANSACTION.RECON", TRX.RECON)
      IF ERR THEN
         MSG = "File Open block experienced an error"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP
      END ELSE
         READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
         READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
         MAT sockPROPS = ""
         MATPARSE sockPROPS FROM PARAMS
         pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = pAns
         pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
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
      FILE = ""
      PRD.LIST = ""
      EOI = DCOUNT(CMD, @FM)
      FOR I = 1 TO EOI
         BEGIN CASE
            CASE UPCASE(FIELD(CMD<I>, "=", 1)) = "FILE"
               FILE = FIELD(CMD<I>, "=", 2)
            CASE UPCASE(FIELD(CMD<I>, "=", 1)) = "PRODUCT"
               PRD.LIST = FIELD(CMD<I>, "=", 2)
         END CASE
      NEXT I
      MSG.ISO = ""
      IF FILE = "" THEN 
         ERR = "No file given"
      END ELSE
         IF FILE = "RESET" THEN
            CLEARFILE TRX.RECON
            MSG = "TRANSACTION.RECON has been cleared."
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
      isEXT   = 0
      IF FIELD(FILE, ".", 2) = "EXT" THEN isEXT = 1
      SELECT IOFILE
      LOOP
         READNEXT ID ELSE EXIT
         CNT1 += 1
         IF MOD(CNT1, SHOWAT)=0 THEN
            MSG = "Checked ":CNT1:" records and processed ":OCONV(CNT2, "MD0,")
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
         READ TREC FROM IOFILE, ID  ELSE CONTINUE
         COUNTER = 0
         ACCR.INT= 0
         TRAN.AMT= 0
         BALANCE = 0
         BEGIN CASE
            CASE isEXT
               ACCR.INT= ""
               BALANCE = ""
               FR = 1
               GOSUB SUM..TRAN..AMT   ;* returns COUNTER and TRAN.AMT
               GOSUB WRITE..RECON
            CASE NOT(isEXT)
               ACCR.INT= TREC<5,2,1>
               BALANCE = TREC<1>
               FR = 17
               GOSUB SUM..TRAN..AMT
               GOSUB WRITE..RECON
         END CASE
         CNT2 += 1
      REPEAT
      MSG = "Finished ........................................................"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG.ISO:MSG)
      MSG = "Checked ":CNT1:" records and processed ":OCONV(CNT2, "MD0,")
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG.ISO:MSG)
      FN = FILEINFO(IOFILE, 1)
      IF FN[1,3] = "upl" THEN DELETE VOC, FN
      STOP
      * --------------------------------------------------------------------
SUM..TRAN..AMT:
      COUNTER = 0
      TRAN.AMT= 0
      EOR = DCOUNT(TREC, @FM)
      FOR R = FR TO EOR
         COUNTER += 1
         TRAN.AMT+= TREC<R, 3, 1>
      NEXT R
      RETURN
      * --------------------------------------------------------------------
WRITE..RECON:
      KEY = FILE:"*":THIS.PROD:"*":ID
      REC = DATE():"*":TIME():@FM
      REC:= OCONV(BALANCE, "MD2"):@FM
      REC:= COUNTER:@FM
      REC:= OCONV(ACCR.INT, "MD3"):@FM
      REC:= OCONV(TRAN.AMT, "MD2")
      WRITE REC ON TRX.RECON, KEY
      RETURN
   END