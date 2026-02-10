      * uSTREAMS: Catch data as it happens.           
      * ----------------------------------------------
      COMMON /uCDC/ DELTAS, dot, iCTR, fCTR, maxOut, IAM
      * ----------------------------------------------
$INCLUDE I_Prologue
      *
      GOSUB INITIALISE
      ******* [ Process until stop flag is on] *******
      STOP.ME = 0
      GOSUB LOOP..CONTROL
      LOOP
         SELECT DELTA.LOG
         LOOP
            READNEXT KEY      ELSE EXIT
            READU    NewRec   FROM DELTA.LOG, KEY ELSE CONTINUE
            DELETE   DELTA.LOG, KEY
            RELEASE  DELTA.LOG, KEY
            
            * -------------------------------------------------------
            * KEY= DATE():dot:TIME():dot:u2File:dot:ID               
            * NewRec <1>   An Error Message - ""                     
            * NewRec <2-n> The NewRec                                
            * Delete NewRec<1> before uing NewRec                    
            * -------------------------------------------------------
            
            TMP   = EREPLACE(KEY, dot, @FM)
            DT    = OCONV(TMP<1>, cDT)
            TM    = OCONV(TMP<2>, cTM)
            AC    = WHOAMI
            FL    = TMP<3>
            ID    = TMP<4>
            
            * -------------------------------------------------------
            
            IF INDEX(FL, ">>", 1) THEN
               TMP= EREPLACE(FL, ">>", @FM)
               AC = TMP<1>
               FL = TMP<2>
            END
            * -------------------------------------------------------
            ERR.REC = NewRec<1>
            IF TRIM(ERR.REC) # "" THEN
               WRITE ERR.REC ON DELTA.ERRS, KEY
            END
            
            NewRec = DELETE(NewRec, 1,0,0)
            IF NewRec # "" THEN
               NewRec = EREPLACE(NewRec, @SM, sm)
               NewRec = EREPLACE(NewRec, @VM, vm)
               NewRec = EREPLACE(NewRec, @FM, fm)
               NewRec = ID:im:NewRec          ; * this is clear text
               
               * -------------------------------------------------------
               * uRESPONSES:                                            
               * 1>  Message to send                                    
               * 2>  Queue to send to                                   
               * 3>  CorrelationID                                      
               * 4>  Fmt - leave empty                                  
               * -------------------------------------------------------
               
               CORR.ID = CORR.TEMPLATE
               CORR.ID = EREPLACE(CORR.ID, "$$", FL)
               CORR.ID = EREPLACE(CORR.ID, "@@", AC)
               
               MQ.MSG = MQ.TEMPLATE
               MQ.MSG = EREPLACE(MQ.MSG, "$COR$", CORR.ID)
               MQ.MSG = EREPLACE(MQ.MSG, "$FL$" , FL)
               MQ.MSG = EREPLACE(MQ.MSG, "$AC$" , AC)
               MQ.MSG = EREPLACE(MQ.MSG, "$PAY$", NewRec)
               
               RESP = ""
               RESP<1> = MQ.MSG
               RESP<2> = NRT.QUE
               RESP<3> = CORR.ID
               RESP<4> = RAW
               
               WRITE RESP ON uRESPONSES, CORR.ID
               
               **************************************************
               **         Need to update .LOADED file          **
               **************************************************
               
               ****** [ Debug ONLY ] ******
               MSG = EREPLACE(MSG.TEMPLATE, "$$", CORR.ID)
               MSG = EREPLACE(MSG, "@@", ID)
               CALL uLOGGER (0, LOG.KEY:MSG)
               ****************************
            END
         REPEAT
         RQM
         GOSUB LOOP..CONTROL
      REPEAT
      *
END..PROG:
      EOI = DCOUNT(FNAMES, @FM)
      ERR = ""
      FOR F = 1 TO EOI
         CALL SR.FILE.CLOSE(ERR, FNAMES<F>)
         IF ERR # "" THEN 
            CALL uLOGGER (0, LOG.KEY:ERR)
            ERR = ""
         END
      NEXT F
      IF ERR # "" THEN CALL uLOGGER (0, LOG.KEY:ERR)
      MSG = "uSTREAMS - stopping --------------------------------"
      CALL uLOGGER (0, LOG.KEY:MSG)
      STOP
      *
LOOP..CONTROL:
      READ CHKR FROM BP.UPL, "STOP" ELSE CHKR=""
      IF CHKR="stop" THEN STOP.ME = 1
      RETURN
      *
INITIALISE:
      ERR = ""
      ANS = ""
      NRT.QUE = "022_DONOTUSE_000"
      CORR.TEMPLATE = "uStreams-$$_@@"
      MSG.TEMPLATE = "$$   @@ sent upstream"
      LOG.KEY = "uSTREAMS":@FM
      RAW = "RAW"
      *
      * All templates must be parameterised BEFORE delivery
      *
      MQ.TEMPLATE = "task<is>022<tm>runtype<is>NRT<tm>correlationid<is>$COR$<tm>"
      MQ.TEMPLATE:= "map<is>$FL$_$AC$.map<tm>payload<is>$PAY$<tm>"
      sm = "<sm>"
      vm = "<vm>"
      fm = "<fm>"
      im = "<im>"
      cDT = "D4-"
      cTM = "MTS"
      EXECUTE "WHO" CAPTURING JUNK
      nSPC   = DCOUNT(JUNK, " ")
      WHO    = FIELD(JUNK<1>, " ", nSPC)
      WHOAMI = FIELD(JUNK<1>, " ", 2)
      MSG = "uSTREAMS - starting in ":WHOAMI:" as ":WHO:" ------------------"
      CALL uLOGGER (0, LOG.KEY:MSG)
      *
      CALL SR.OPEN.CREATE (ERR, "uFILES", "DYNAMIC", uFILES)
      IF ERR # "" THEN GO END..PROG
      CALL SR.OPEN.CREATE (ERR, "uRESPONSES", "19", uRESPONSES)
      IF ERR # "" THEN GO END..PROG
      CALL SR.OPEN.CREATE (ERR, "uDELTA.LOG", "DYNAMIC", DELTA.LOG)
      IF ERR # "" THEN GO END..PROG
      CALL SR.OPEN.CREATE (ERR, "uDELTA.ERRS", "DYNAMIC", DELTA.ERRS)
      IF ERR # "" THEN GO END..PROG
      CALL SR.FILE.OPEN (ERR, "BP.UPL", BP.UPL)
      IF ERR # "" THEN GO END..PROG
      RETURN
      *
END 
