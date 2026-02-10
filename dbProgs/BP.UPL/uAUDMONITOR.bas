      SUBROUTINE uAUDMONITOR(ERR, FIL, IID, OLD, NEW)
      COMMON /kiwiAUD/ D.DELTAS, DELTAS, dot, comma, IAM, 
         extn, processor, prefix, atIM, atFM, atVM, atSM
      * ------------------------------------------------
      *        Manual Audit Event Service              *
      * ------------------------------------------------
      IF dot # '.' THEN 
         OPEN "DICT", "uDELTA.LOG" TO D.DELTAS ELSE CRT "Open error"; RETURN
         OPEN "uDELTA.LOG" TO DELTAS ELSE CRT "Open error"; RETURN
         dot = '.'; comma = ','; IAM = @WHO
         atIM = "<im>"; atFM = "<fm>" ; atVM = "<vm>" ; atSM = "<sm>"
         extn = '.ulog'; processor = 'uAUDMONITOR'
         prefix = '@EXCP.'
      END
      ERR = ""
      ORIGINAL = NEW
      LOG.THIS = 1
      READ EXCLUSIONS FROM D.DELTAS, prefix:FIL THEN
         * determines if OLD & NEW are different and
         * sets "LOG.THIS" in the subroutine        
         GOSUB EXCLUDE..DATA
      END
      IF NOT(LOG.THIS) THEN RETURN ;* nothing has changed!
      SEQN = 1
      KEY = DATE():dot:TIME()
      LOOP
         KEY = KEY:dot:SEQN:extn
         READU DREC FROM DELTAS, KEY ELSE
            ORIGINAL = EREPLACE(ORIGINAL, @SM, atSM)
            ORIGINAL = EREPLACE(ORIGINAL, @VM, atVM)
            ORIGINAL = EREPLACE(ORIGINAL, @FM, atFM)
            HDR  = KEY:atIM:IAM:atIM:FIL:atIM:IID:atIM
            NREC = HDR:ORIGINAL
            WRITE NREC ON DELTAS, KEY
            EXIT
         END
         SEQN += 1
         IF SEQN > 999 THEN ERR = "More then 1000 updates in a second." ; EXIT
       REPEAT
      IF ERR # "" THEN CRT processor:" ERROR: ":ERR ; RETURN
      RETURN
      *
      * -----------------------------------------------------------------------------
      *
EXCLUDE..DATA:
      *  EXCLUSIONS is an N attributed list of
      *  a,m,s  values to ignore              
      EOI = DCOUNT(EXCLUSIONS, @FM)
      FOR I = 1 TO EOI
         a = FIELD(EXCLUSIONS<I>, comma, 1)+0
         m = FIELD(EXCLUSIONS<I>, comma, 2)+0
         s = FIELD(EXCLUSIONS<I>, comma, 3)+0
         OLD<a,m,s> = ""
         NEW<a,m,s> = ""
      NEXT I
      IF OLD = NEW THEN LOG.THIS = 0
   END
