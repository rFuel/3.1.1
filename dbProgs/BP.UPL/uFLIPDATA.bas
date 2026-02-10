      SUBROUTINE uFLIPDATA (ANS, FILE, SERIAL, SEQ, UVBLK)
$INCLUDE I_Prologue
      IF UNASSIGNED (DBT) OR DBT=0 THEN  EXECUTE "CLEAR.COMMON"
      *
      LOG.KEY = "uplLOG":@FM
      LVL = 0
      ANS = ""
      OPEN "BP.UPL" TO BP.UPL THEN
         READ DBT FROM BP.UPL, "DBT" ELSE DBT="UV"
         READ STP FROM BP.UPL, "STOP" ELSE STP=""
         IF STP = "stop" THEN 
            ANS = "<<FAIL>> STOP switch is set on."
            GO RETN
         END
      END ELSE
         ANS = "<<FAIL>> BP.UPL cannot be accessed!"
         GO RETN
      END
      IF ANS#"" THEN GO RETN
      IF DBT="UV" THEN 
         EXE = "COPYI "
      END ELSE
         EXE = "COPY "
      END
      FILE = EREPLACE(FILE, " ", "_")
      EXE := "FROM ":FILE:".TAKE TO ":FILE:".LOADED ALL OVERWRITING DELETING"
      CALL uLOGGER(LVL, LOG.KEY:"<< uFLIPDATA**************************************************")
      CALL uLOGGER(LVL, LOG.KEY:EXE)
      EXECUTE EXE CAPTURING JUNK
      JUNK = EREPLACE(JUNK, @FM," ")
      CALL uLOGGER(LVL, LOG.KEY:JUNK)
      CALL uLOGGER(LVL, LOG.KEY:">> uFLIPDATA**************************************************")
RETN:
      IF ANS # "" THEN CALL uLOGGER(LOG.KEY, ANS)
$IFNDEF isRT
      CLOSE
$ENDIF
      RETURN
   END
