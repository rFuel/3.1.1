      SUBROUTINE SR.GETDELTAKEYS (ANS, FILE, MAX, MSG, OLDstr, NEWstr)
$INCLUDE I_Prologue
      * Purpose:                                                
      * This subroutine is called by java class uDeltaService   
      * It will reserve MAX records for Delta2kafka.
      * uDelta2kafka (threads) calls SR.DELETEKEYS to tidyup
      * uDelta2kafka (threads) calls SR.RECLAIMKEYS to tidyup
      * This is neccessary for isolation and restart / recovery 
      * --------------------------------------------------------
      * Functions:                                              
      * Reserve MAX records from FILE by changing their @IDs    
      * Change the keys from OLDstr to NEWstr in the @ID        
      *                                                         
      * 1.  Reclaim any unprocessed records which were reserved 
      *     the last time this subroutine was called by the SAME
      *     uDeltaService process. This happens when the process
      *     is interrupted by loss of connection with Kafka.    
      *                                                         
      * 2.  Reclaim any other unprocessed records which were    
      *     left behind by uDeltaService processes which also   
      *     have been stopped  - for any number of reasons.     
      *                                                         
      * 3.  Claim a sample of MAX records for this process !    
      * --------------------------------------------------------
      *                                                         
      * CLAIMing records is done by changing the @ID so that    
      * any other or future select statements pass them by.     
      *                                                         
      * uDELTA.LOG @ID: Date.Time.tick.pid.Seqn."ulog"          
      * reserved   @ID: Date.Time.tick.pid.Seqn.{NEWstr}.date.time
      *                                                         
      *   Date.Time  are of the audit log EVENT                 
      *   date.time  is when this subr reserved the records     
      * --------------------------------------------------------
      *
      ORPHANS = ANS+0
      COLLECTOR = MSG+0
      ANS = ""
      MSG = ""
      ULOG= ".ulog"
      NL  = CHAR(10)
      SM  = "<sm>"
      VM  = "<vm>"
      FM  = "<fm>"
      TM  = "<tm>"
      DOT = "."
      CALL SR.FILE.OPEN(ERR, FILE, IOFILE)
      IF ERR THEN RETURN
      *
      * --------------------------------------------------------
      * 1. Reclaim unprocessed records before getting new ones  
      * --------------------------------------------------------
      *
      IF COLLECTOR THEN
         MSG := OCONV(DATE(), "MD2"):" ":OCONV(TIME(), "MTS"):"  collecting left-overs.":NL
         CMD = "SELECT ":FILE:"  LIKE ...":NEWstr:"..."
         EXECUTE CMD CAPTURING JUNK
         CNT=0
         LOOP
            READNEXT ID ELSE EXIT
            READ REC FROM IOFILE, ID ELSE CONTINUE
            CNT+=1
            REC = EREPLACE(REC, @SM, SM)
            REC = EREPLACE(REC, @VM, VM)
            REC = EREPLACE(REC, @FM, FM)
            ANS := ID:TM:REC:NL
         REPEAT
         MSG := OCONV(DATE(), "MD2"):" ":OCONV(TIME(), "MTS"):"  collected ":CNT:" left-overs.":NL
         IF ANS # "" THEN
             RETURN
         END
      END
      *
      * --------------------------------------------------------
      * 2. Reclaim orphaned / unprocessed records
      *    CANNOT execute this IF multi-uDeltaService ops
      * --------------------------------------------------------
      *
      IF ORPHANS THEN
         MSG := "***  reclaiming orphans.":NL
         TODAY    = DATE()
         NOW      = TIME()
         CNT      = 0
         CMD = "SELECT ":FILE:"  UNLIKE ...":ULOG:"..."
         EXECUTE CMD CAPTURING JUNK
         LOOP
            READNEXT ID ELSE EXIT
            IF INDEX(ID, ULOG, 1) THEN CONTINUE
            * is it an orphan? ------------------------------------
            EXPIRE.DATE = FIELD(ID, DOT, 7)  ;* Reserver date
            EXPIRE.TIME = FIELD(ID, DOT, 8)  ;* Reserver time
            RESET = 0
            IF EXPIRE.DATE < TODAY THEN RESET = 1
            IF EXPIRE.TIME < NOW   THEN RESET = 1
            IF NOT(RESET) THEN CONTINUE;
            * -----------------------------------------------------
            nID  = ID[1, INDEX(ID, DOT, 5)-1]:ULOG
            READU REC FROM IOFILE, ID THEN
               WRITE REC ON IOFILE, nID
               DELETE IOFILE, ID
               CNT+=1
            END ELSE
               RELEASE IOFILE, ID
            END
         REPEAT
         MSG := "***  reset ":CNT:" orphans.":NL
         IF CNT # 0 THEN MSG := "Reset ":CNT:" orphaned events":NL
      END
      *
      * --------------------------------------------------------
      * 3. Claim a sample size of MAX records for processing    
      * --------------------------------------------------------
      *
      BASE.DTM = "NOW"
      OPER     = "ADD"
      INTERVAL = "60"               ;* hold for 1 min - reclaim will release them
      PERIOD   = "s"                ;* seconds
      RESULT   = ""
      ERR      = ""
      CALL SR.DTMATH (ERR, BASE.DTM, OPER, INTERVAL, PERIOD, RESULT)
      IF ERR # "" THEN RETURN
      EXPIRE.DT   = FIELD(RESULT, "_", 1)
      EXPIRE.TM   = FIELD(RESULT, "_", 2)
      RESERVED = DOT:EXPIRE.DT:DOT:EXPIRE.TM
      CMD = "SELECT ":FILE:" LIKE ...":OLDstr       ;*  :" SAMPLE ":MAX
      EXECUTE CMD CAPTURING OUTPUT
      CNT = 0
      LOOP
         READNEXT OLD.ID ELSE EXIT
         READU REC FROM IOFILE, OLD.ID LOCKED
            RELEASE IOFILE, OLD.ID
            CONTINUE
         END ELSE
            RELEASE IOFILE, OLD.ID
            CONTINUE
         END
         NEW.ID = EREPLACE(OLD.ID, OLDstr, NEWstr)
         NEW.ID:= RESERVED
         DELETE IOFILE, OLD.ID
         WRITE REC ON IOFILE, NEW.ID
         REC = EREPLACE(REC, @SM, SM)
         REC = EREPLACE(REC, @VM, VM)
         REC = EREPLACE(REC, @FM, FM)
         ANS := NEW.ID:TM:REC:NL
         CNT+=1
         IF CNT >= MAX THEN EXIT
      REPEAT
      *
      RETURN
   END
