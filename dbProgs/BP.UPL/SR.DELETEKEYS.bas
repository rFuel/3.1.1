      SUBROUTINE SR.DELETEKEYS (ANS, FILE, KEYS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * delete all the items in KEYS from FILE                  
      * --------------------------------------------------------
      NL  = CHAR(10)
      SM  = "<sm>"
      VM  = "<vm>"
      FM  = "<fm>"
      TM  = "<tm>"
      CALL SR.FILE.OPEN(ERR, FILE, IOFILE)
      ANS = ERR
      IF ERR THEN RETURN
      ANS = ""
      *
      * ---------------------------------------------------------
      *
      ITEMS = EREPLACE(KEYS, TM, @FM)
      LOOP
         ID = REMOVE(ITEMS, STAT)
         OKAY = 0
         IF STAT > 0 THEN OKAY = 1
         IF ID # "" THEN OKAY  = 1
      WHILE (OKAY) DO
         IF ID = "" THEN CONTINUE
         PROCESS = 1
         READU JUNK FROM IOFILE, ID LOCKED
            PROCESS = 0
         END ELSE
            PROCESS = 0
         END
         IF NOT(PROCESS) THEN
            RELEASE IOFILE, ID
            CONTINUE
         END
         * -----------------------------------------------------
         *  LOCKED: allow UV to wait and try again
         * -----------------------------------------------------
         DELETE IOFILE, ID
         RELEASE IOFILE, ID
      REPEAT
      RETURN
   END
