SUBROUTINE uCDC(TrigName, Schema, u2File, Event, eTime, NewID, NewRec, OldID, OldRec, Assoc, Assoc.Event, Count, Chain.Cascade, Cascade)
      * ------------------------------------------------
      COMMON /uCDC/ DELTAS, dot, IAM, atIM, atFM, atVM, atSM
      * ------------------------------------------------
      IF OldID='' THEN ID=NewID ELSE ID=OldID
      IF ID='' THEN RETURN
      IF dot # '<|>' THEN 
         OPEN "uDELTA.LOG" TO DELTAS ELSE RETURN
         dot = '<|>'; atIM = "<im>"; atFM = "<fm>" ; atVM = "<vm>" ; atSM = "<sm>"
         IAM = @WHO
      END
      EXTN = ".ulog"
      *
      LCK.CNT=0
      eDate = OCONV(DATE(), "D4-DMY[2,2,4]")
      YY    = FIELD(eDate, "-", 3)
      MM    = FIELD(eDate, "-", 2)
      DD    = FIELD(eDate, "-", 1)
      eDate = YY:MM:DD
      eTime = EREPLACE(OCONV(TIME(), "MTS"), ":", "")
      LOOP
         LCNT = ("000":LCK.CNT) "R#3"
         KEY  = eDate:eTime:LCNT:EXTN
         READU MT.REC FROM DELTAS, KEY ELSE 
            HDR  = IAM:atIM:u2File:atIM:NewID:atIM
            NREC = HDR:NewRec
            NREC = EREPLACE(NREC, @SM, atSM)
            NREC = EREPLACE(NREC, @VM, atVM)
            NREC = EREPLACE(NREC, @FM, atFM)
            WRITE NREC ON DELTAS, KEY
            RELEASE DELTAS, KEY
            EXIT
         END
         LCK.CNT+=1
      REPEAT
      RELEASE DELTAS, KEY
      RETURN
   END
