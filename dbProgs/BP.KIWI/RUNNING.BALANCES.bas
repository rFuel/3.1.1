$INCLUDE I_Prologue
      PROMPT ""
      *
      * RUNNING.BALANCES {product}
      * --------------------------------------------------------------------------------
      * Developed during the S4 product migrations - expand for other products
      * --------------------------------------------------------------------------------
      * Select all accounts in the product
      * Obtain either Balance or OpeningBalance and LastTranID
      * Read Tran record;
      *    set Balance
      *    loop through transactions and load into an array;
      *    <1, 1-n> TranID      (SORTED largest to smallest)
      *    <2, 1-n> Tran Code
      *    <3, 1-n> TranValue   (amount * (1 or -1)
      *                         (NB TranCode '40' CAN have a value)
      *    <4, 1-n> RunningBal  0 until the end
      * Get ALL the TRAN.EXT items for the account
      *     load the transactions into the array above
      *
      * Loop through the array, stating with Tran Balance
      *     apply TranValue to the first element, giving RunningBal and update the array
      *
      * --------------------------------------------------------------------------------
      * write the array to TRANSACTION.BALANCES with the AccountID as the key.
      * --------------------------------------------------------------------------------
      *
      ERR = ""
      CALL SR.FILE.OPEN (ERR, "VOC"                 , VOC          ) ; IF ERR # "" THEN GO END..PROG
      CALL SR.FILE.OPEN (ERR, "TRAN"                , TRAN         ) ; IF ERR # "" THEN GO END..PROG
      CALL SR.FILE.OPEN (ERR, "TRANSACTION.BALANCES", BALANCES     ) ; IF ERR # "" THEN GO END..PROG
      *
      isACTIVE = 0
      FOR L = 0 TO 10
         IF SELECTINFO(L, 1) THEN isACTIVE=1; EXIT
      NEXT
      SEL.LIST = ""
      IF INDEX(@SENTENCE, "+TARC", 1) THEN doTARC = 1 ELSE doTARC = 0
      IF isACTIVE THEN 
         READLIST SEL.LIST ELSE SEL.LIST = ""
      END ELSE
         *
         ACCOUNT = "ALL"
         CMD = EREPLACE(@SENTENCE, " ", @FM)
         LOCATE "RUNNING.BALANCES" IN CMD <1> SETTING FND THEN
            PRODUCT = CMD<FND+1>
            CHECK = PRODUCT[1,1]
            IF NOT(INDEX("SLI", CHECK, 1)) THEN ACCOUNT = PRODUCT
         END ELSE
            PRODUCT = "S4"                      ;* Should probably stop
         END
      END
      *
      DUMMY = @(0,0)
      LOG.KEY  = "TranBals":@FM
      BASE.DATE = 18080                     ;* 01/07/2017
      *
      LOG.MSG = "-------------------------------------------------------------------------"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      STARTED = TIME()
      TM = TIME()
      DP = FIELD(TM, ".", 2)
      TM = OCONV(TM, "MTS"):".":DP
      LOG.MSG = "Started RUNNING.BALANCES at ":TM:" on ":OCONV(DATE(), "D4-")
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      LOG.MSG = "-------------------------------------------------------------------------"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      *
      TARC.FILE.ARRAY = ""
      IF NOT(doTARC) THEN GO END..OF..TARCS
      * --------------------------------------------------------------------------------
      LOG.MSG = "   Finding the TARC files for use"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      DATE.CNT  = DATE() + 1
      THIS.YEAR = FIELD(OCONV(DATE.CNT, "D4-"), "-", 3)+0
      TARC.CHK  = "TRAN.ARC.$.#"
      DOLLAR    = "$"
      HASH      = "#"
      *
      * -----------------------------------------------------------------------------------
      * Get 8 years of archived transactions
      * -----------------------------------------------------------------------------------
      *
      LOOP
         DATE.CNT -= 1
         WORK.YEAR = FIELD(OCONV(DATE.CNT, "D4-"), "-", 3)+0
         IF THIS.YEAR - WORK.YEAR > 8 THEN 
             JUNK = ""
            EXIT
         END
         LOOP.CNT = 0                 ;* Stops an endless loop if no TARC file is found
         * --------------------------------------------------------------------------------
         * Now, find the TARC files by date
         * --------------------------------------------------------------------------------
         FOUND = 0
         TARC = EREPLACE(TARC.CHK, DOLLAR, DATE.CNT)
         TARC = EREPLACE(TARC    , HASH  , 1)
         * --------------------------------------------------------------------------------
         * iS the TARC file in the VOC ?
         *
         READ JUNK FROM VOC, TARC THEN
            * -----------------------------------------------------------------------------
            * Can the TARC file be opened ?
            * -----------------------------------------------------------------------------
            ERR = ""
            IF ERR = "" THEN 
               * --------------------------------------------------------------------------
               * We want the data in this TARC file !
               * --------------------------------------------------------------------------
               TARC.FILE.ARRAY<-1> = TARC
               FILE.EXT = 1
               LOOP 
                  * -----------------------------------------------------------------------
                  * Get the next TARC file for this date  i.e. .1 .2 .3 .4 etc.
                  * -----------------------------------------------------------------------
                  FILE.EXT += 1
                  TARC = EREPLACE(TARC.CHK, DOLLAR, DATE.CNT)
                  TARC = EREPLACE(TARC    , HASH  , FILE.EXT)
                  FOUND = 0
                  READ JUNK FROM VOC, TARC ELSE EXIT
                  TARC.FILE.ARRAY<-1> = TARC
               REPEAT
               *
               DATE.CNT -= 1
            END
         END
         LOOP.CNT += 1
         IF LOOP.CNT > 200 THEN EXIT
      REPEAT
      LOG.MSG = "   ":DCOUNT(TARC.FILE.ARRAY, @FM):"  TARC files added"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      *
END..OF..TARCS:
      *
      LOG.MSG = "   Done.... Processing now..."
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      * -----------------------------------------------------------------------------------
      *
      IF NOT(isACTIVE) THEN
         IF ACCOUNT = "ALL" THEN
            CLEARFILE BALANCES
            EXE = "SELECT TRAN LIKE ...":PRODUCT:" OR LIKE ...":PRODUCT:".... OR LIKE ...":PRODUCT:"/..."
            EXECUTE EXE CAPTURING JUNK
            READLIST SEL.LIST ELSE SEL.LIST = ""
         END ELSE
            EXE = "Nothing, use account: ":ACCOUNT
            SEL.LIST = ACCOUNT
         END
         IF INF.LOGGING THEN
            LOG.MSG = "   Execute: ":EXE
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            CRT LOG.MSG
            LOG.MSG = "   Returned ":OCONV(DCOUNT(SEL.LIST, @FM), "MD0,"):" records"
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            CRT LOG.MSG
         END
      END ELSE
         LOG.MSG = "   Using active select list"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         CRT LOG.MSG
         LOG.MSG = "   Which has ":OCONV(DCOUNT(SEL.LIST, @FM), "MD0,"):" records"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         CRT LOG.MSG
      END
      *
      RCNT = 0      ;* records read
      PCNT = 0      ;* records processed
      *
      * -----------------------------------------------------------------------------------
      * 1. Process TRAN record(s)
      * -----------------------------------------------------------------------------------
      *
      THIS.FILE = "TRAN"
      LOOP
         *
         ID = REMOVE(SEL.LIST, PSTATUS)
         IF NOT(PSTATUS) AND ID = "" THEN EXIT
         *
         READ TR.REC FROM TRAN, ID ELSE CONTINUE
         THIS.ID = ID 
         RCNT+=1
         BAL = TR.REC<1,1,1>
         ARR = ""
         UPD.REC = TR.REC
         SOT = 17                            ;* Start of Transactions
         EOT = DCOUNT(TR.REC, @FM)           ;* End   of transactions
         GOSUB UPDATE..ARR
         *
         * --------------------------------------------------------------------------------
         * 2. Process TRAN.EXT record(s)
         * --------------------------------------------------------------------------------
         *
         THIS.FILE = "TRAN.EXT"
         CLID = FIELD(FIELD(FIELD(ID, "I", 1), "L", 1), "S", 1)
         CLX = LEN(CLID)
         TNBR = CLID[CLX, 1] + 1
         CALL SR.FILE.OPEN (ERR, "TRAN":TNBR:".EXT" , TRAN.EXT ) ; IF ERR # "" THEN GO END..PROG
         EXT = TR.REC<2,7,0>
         LOOP
            EXT.ID = ID:"/":EXT
            READ TE.REC FROM TRAN.EXT, EXT.ID THEN
               THIS.ID = EXT.ID
               UPD.REC = TE.REC
               SOT = 1
               EOT = DCOUNT(TE.REC, @FM)
               GOSUB UPDATE..ARR
            END ELSE
               EXIT
            END
            EXT += 1
         REPEAT
         *
         * --------------------------------------------------------------------------------
         * 3. Process TRAN.ARC record(s)
         * --------------------------------------------------------------------------------
         *
         F.NBR = 0
         LOOP
            F.NBR += 1
            TARC  = TARC.FILE.ARRAY<F.NBR>
            FOUND = LEN(TARC)
            ACC.EXT = 0
            IF FOUND THEN
               ** DO NOT use sr.file.open - too many TARC files for named common
               OPEN TARC TO TARC.IO ELSE CONTINUE
               LOOP
                  ACC.EXT += 1
                  TARC.ID = ID:"/":ACC.EXT
                  READ TA.REC FROM TARC.IO, TARC.ID THEN
                     THIS.ID = TARC.ID
                     UPD.REC = TA.REC
                     SOT = 2
                     EOT = TA.REC<1> + 1               ;* holds the number of transaction in the record.
                     GOSUB UPDATE..ARR
                  END ELSE
                     IF ASSIGNED(TARC.IO) THEN CLOSE TARC.IO
                     EXIT
                  END
               REPEAT
               IF ASSIGNED(TARC.IO) THEN CLOSE TARC.IO
            END ELSE
               IF ASSIGNED(TARC.IO) THEN CLOSE TARC.IO
               EXIT
            END
            JUNK = ""
         REPEAT
         IF ASSIGNED(TARC.IO) THEN CLOSE TARC.IO
         *
         * -----------------------------------------------------------------------------
         * 4. Sort the array by Transaction ID
         *    NB: The tran IDs in ARR<1,n> are sorted Ascending Right (as numbers)
         *        smallest in <1,1> largest in <1,n> so MUST work backwards through ARR
         * -----------------------------------------------------------------------------
         *
         SAV = ARR
         ARR = ""
         EOA = DCOUNT(SAV<1>, @VM) + 1
         LOOP 
            EOA -= 1
         WHILE EOA > 0 DO
            TID = SAV<1,EOA>
            TCD = SAV<2,EOA>
            VAL = SAV<3,EOA>
            LOCATE TID IN ARR <1,1> BY "AR" SETTING FND ELSE
               ARR = INSERT(ARR, 1, FND, 0, TID)    ;* Tran ID
               ARR = INSERT(ARR, 2, FND, 0, TCD)    ;* Tran Code
               ARR = INSERT(ARR, 3, FND, 0, VAL)    ;* Tran Value
            END
         REPEAT
         *
         * -----------------------------------------------------------------------------
         * 5. Apply tran values in ARR<3,n> to BAL to obtain the running balance
         *    and store it in ARR<4,n>.
         * -----------------------------------------------------------------------------
         *
         EOA = DCOUNT(ARR<1>, @VM)
         FOR A = EOA TO 1 STEP -1
            ARR<4,A> = BAL
            BAL = BAL - ARR<3,A>
         NEXT A
         WRITE ARR ON BALANCES, ID
         PCNT+=1
         *
         IF MOD(RCNT, 100)=0 THEN 
            TM = TIME()
            DP = FIELD(TM, '.', 2)
            NOW = "   >> ":OCONV(TM, "MTS"):".":DP:"  "
            LOG.MSG = NOW:"Read ":OCONV(RCNT, "MD0,") "R#10":"  Processed ":OCONV(PCNT, "MD0,") "R#10"
            IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            CRT LOG.MSG
         END
      REPEAT
      TM = TIME()
      DP = FIELD(TM, '.', 2)
      NOW = "   >> ":OCONV(TM, "MTS"):".":DP:"  "
      LOG.MSG = NOW:"Read ":OCONV(RCNT, "MD0,") "R#10":"  Processed ":OCONV(PCNT, "MD0,") "R#10"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
END..PROG:
      *
      LOG.MSG = "-------------------------------------------------------------------------"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      TM = TIME()
      DP = FIELD(TM, ".", 2)
      TM = OCONV(TM, "MTS"):".":DP
      LOG.MSG = "Finished RUNNING.BALANCES at ":TM:" on ":OCONV(DATE(), "D4-")
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      LOG.MSG = "-------------------------------------------------------------------------"
      IF INF.LOGGING THEN CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      CRT LOG.MSG
      FINISHED = TIME()
      LAPS = FINISHED - STARTED
      CRT "Runtime: ":LAPS:" seconds"
      *
      IF ERR # "" THEN CRT ERR
      STOP
      *
UPDATE..ARR:
      *
      FOR ATR = SOT TO EOT
         AMT = UPD.REC<ATR, 3, 1> + 0           ;* Transaction amount
         TID = UPD.REC<ATR, 2, 6>               ;* Transaction ID
         TCD = UPD.REC<ATR, 1, 1>               ;* Transaction code
         NBR = TCD[1,1]
         IF NBR >= 5 THEN DC = -1 ELSE DC = 1
         VAL = AMT * DC
         LOCATE TID IN ARR <1,1> SETTING FND ELSE
            ARR<1> = TID:@VM:ARR<1>
            ARR<2> = TCD:@VM:ARR<2>
            ARR<3> = VAL:@VM:ARR<3>
         END
      NEXT
      RETURN
   END
