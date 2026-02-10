      SUBROUTINE SR.RECLAIMKEYS (ANS, FILE, KEYS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * reclaim unprocessed items in KEYS from FILE
      * --------------------------------------------------------
      DOT = "."
      TM  = "<tm>"
      ULOG= ".ulog"
      CALL SR.FILE.OPEN(ERR, FILE, IOFILE)
      ITEMS = EREPLACE(KEYS, TM, @FM)
      CNT=0
      LOOP
         ID = REMOVE(ITEMS, STAT)
         OKAY = 0
         IF STAT > 0 THEN OKAY = 1
         IF ID # "" THEN OKAY  = 1
      WHILE (OKAY) DO
         nID  = ID[1, INDEX(ID, DOT, 5)-1]:ULOG
         READU REC FROM IOFILE, ID LOCKED
            RELEASE IOFILE, ID
            CONTINUE
         END ELSE
            RELEASE IOFILE, ID
            CONTINUE
         END
         WRITE REC ON IOFILE, nID
         DELETE IOFILE, ID
         RELEASE IOFILE, ID
         CNT+=1
      REPEAT
      RETURN
   END

