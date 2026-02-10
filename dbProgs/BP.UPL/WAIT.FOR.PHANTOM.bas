$INCLUDE I_Prologue
      PROMPT ""
      DUMMY = @(0,0)
      * ------------------------------------------------------------------
      CMD = EREPLACE(@SENTENCE, " ", @FM)
      LOCATE("WAIT.FOR.PHANTOM", CMD; AV) ELSE AV = 99
      PROC = CMD<AV+1>
      IF PROC = "" THEN
         CRT
         CRT "[FATAL] Usage: WAIT.FOR.PHANTOM {ph-name}"
         CRT
         STOP
      END
      * ------------------------------------------------------------------
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "Still waiting for phantom ":PROC
      CMD = 'SH -c"ps -ef | grep phantom | grep ':PROC:' | grep -v grep"'
      SHOW.AT = 60
      SHOW.CT = 0
      * ------------------------------------------------------------------
      LOOP
         EXECUTE CMD CAPTURING OUTPUT
         IF OUTPUT = "" THEN EXIT
         RQM
         NAP 2
         SHOW.CT += 1
         IF NOT(MOD(SHOW.AT,SHOW.CT)) THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      REPEAT
      * ------------------------------------------------------------------
      CRT "[DONE] no phantoms for ":PROC
      STOP
   END
