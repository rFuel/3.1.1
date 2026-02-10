      SUBROUTINE uAES(ERR, DTS, ACT, FIL, IID, NewRec, EXTN)
      COMMON /uCDC/ DELTAS, dot, IAM, atIM, atFM, atVM, atSM
      * ------------------------------------------------
      *           Audit Event Service                  *
      * ------------------------------------------------
      IF dot # '<|>' THEN 
         OPEN "uDELTA.LOG" TO DELTAS ELSE RETURN
         dot = '<|>'; atIM = "<im>"; atFM = "<fm>" ; atVM = "<vm>" ; atSM = "<sm>"
         IAM = @WHO
      END
      ERR = "uAES called with empty fields"
      IF IID='' THEN RETURN
      IF FIL='' THEN RETURN
      IF ACT='' THEN RETURN
      IF DTS='' THEN RETURN
      ERR = ""
      *
      KEY  = DTS:EXTN
      READU MT.REC FROM DELTAS, KEY THEN
         RELEASE DELTAS, KEY
      END ELSE 
         HDR  = DTS:atIM:ACT:atIM:FIL:atIM:IID:atIM
         NREC = HDR:NewRec
         NREC = EREPLACE(NREC, @SM, atSM)
         NREC = EREPLACE(NREC, @VM, atVM)
         NREC = EREPLACE(NREC, @FM, atFM)
         WRITE NREC ON DELTAS, KEY
         RELEASE DELTAS, KEY
      END
      RETURN
   END
