      SUBROUTINE SR.READRECORD (MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * --------------------------------------------------------
      PRECISION 9
      STX = TIME()
      DIM IN.STRINGS(20)
      EQU REPLY TO IN.STRINGS(1)
      EQU FILE  TO IN.STRINGS(2)
      EQU VPOOL TO IN.STRINGS(3)
      EQU MPOOL TO IN.STRINGS(4)
      EQU DPOOL TO IN.STRINGS(5)
      EQU EIOBJ TO IN.STRINGS(6)                 ; * true / false
      DIM REC.ARR(100)
      DIM FILE.ARR(100)
      DIM ID.ARR(100)
      DIM WV.ARR(100)
      DIM WU.ARR(100)
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   SR.READRECORD Started for file  [":FILE:"]"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      * --------------------------------------------------------
      
      RETURN
   END
