      SUBROUTINE uTRIGGER(TrigName, Schema, u2File, Event, eTime, NewID, NewRec, OldID, OldRec, Assoc, Assoc.Event, Count, Chain.Cascade, Cascade)
      * -----------------------------------------------------
      COMMON /uplTRIGCOM/ TFILE, LOGIO, LASTNUM, SPARE01, SPARE02, SPARE03, SPARE04, SPARE05
      * -------------------------------------------------------
      *  MUST create-file TRIGGERS DYNAMIC                     
      *       create-file uLOG 19                              
      *  BEWARE of permissions to uLOG !! must have write perms
      * -------------------------------------------------------
      IF NOT(FILEINFO(TFILE, 0)) THEN 
         SPARE01=""; SPARE02=""; SPARE03=""; SPARE04=""; SPARE05=""
         LASTNUM = 1
         OPEN "TRIGGERS" TO TFILE ELSE RETURN
         OPEN "uLOG" TO IOF ELSE RETURN
         OPENSEQ "uLOG", "uTriggers"  TO LOGIO ELSE
            WRITE "" ON IOF, "uTriggers" ELSE RETURN
            OPENSEQ "uLOG", "uTriggers"  TO LOGIO ELSE RETURN
         END
         FOR I = 1 TO 10
            LASTNUM=I
            READV CHK FROM IOF, "uTriggers.":I, 2 ELSE I=999
         NEXT I
         CLOSE IOF
      END
      KEY = u2File:"*":UPCASE(eTime:"*":Event)
      *
      READ CALL.LIST FROM TFILE, KEY ELSE RETURN
      EOI = DCOUNT(CALL.LIST, @FM)
      FOR I = 1 TO EOI
         NAME = CALL.LIST<I,1,1>
         SUBR  = CALL.LIST<I,2,1>
         IF NAME = "" THEN NAME = TrigName
         IF SUBR = "" THEN CONTINUE
         ***************************************************************
         CNT=0
10       *
         CNT+=1
         IF CNT < 5 THEN
            STATUS lSTATS FROM LOGIO ELSE lSTATS=""
            IF lSTATS<6> > 10000000 THEN
               CLOSESEQ LOGIO
               OPEN "uLOG" TO IOF THEN
                  READU OLDREC FROM IOF, "uTriggers" ELSE OLDREC=""
                  WRITE OLDREC ON IOF, "uTriggers.":LASTNUM
                  OLDREC = ""
                  WRITE "" ON IOF, "uTriggers" ELSE RETURN
                  RELEASE IOF
                  CLOSE IOF
                  OPENSEQ "uLOG", "uTriggers"  TO LOGIO ELSE RETURN
                  LASTNUM+=1
                  IF LASTNUM > 10 THEN LASTNUM=1
                  GO 10
               END
               RETURN      ;* TO CALLING PROGRAM
            END
            SEEK LOGIO, 0,2 ELSE NULL
            LOG.MSG = TIMEDATE():" ":TrigName:" invoked ":B4Aftr:" ":Event
            LOG.MSG:= " on File ":u2File:" for item ":NewID
            WRITESEQ LOG.MSG TO LOGIO ELSE NULL
         END
         ***************************************************************
         ** if logs stop, it's because of an lSTATS issue !             
         ***************************************************************
         CALL @SUBR(NAME, Schema, u2File, Event, eTime, NewID, NewRec, OldID, OldRec, Assoc, Assoc.Event, Count, Chain.Cascade, Cascade)
      NEXT I
      RETURN
      * -----------------------------------------------------
   END
