      SUBROUTINE SR.DV.SUBSTITUTIONS (VPOOL, MPOOL, DPOOL, ARGS, ARR)
$INCLUDE I_Prologue
      * --------------------------------------------------------------------- *
      * Substitute variables in ARGS with data, return as ARR ::              *
      * --------------------------------------------------------------------- *
      *                                                                        
      * ARGS   : SV list of variables and constatnts                           
      *        : replace the variables with data                               
      *                                                                        
      * ARR    : FM list of substituted data                                   
      *                                                                        
      SEP = "<tm>"
      CMD = ""
      RTN.STRING = ""
      CALL SR.GET.INSTRINGS (RTN.STRING , ARGS , SEP , CMD)
      ARGS = CMD
      CONVERT @FM TO @SM IN ARGS
      ARR = ""; 
      PRECISION 9
      STX = TIME()
      NBR.ARGS = DCOUNT(ARGS, @SM)
      FOR A = 1 TO NBR.ARGS
         ARG = TRIM(ARGS<1,1,A>)
         BEGIN CASE
            CASE ARG = "@VPOOL"
               DAT = VPOOL
               CONVERT @VM TO CHAR(9) IN DAT
            
            CASE INDEX(ARG, "@VPOOL", 1)
               IDX = INDEX(ARG, "@VPOOL", 1) -1
               DAT = ARG[1,IDX]:VPOOL
            
            CASE ARG = "@MPOOL"
               DAT = CONVERT(@VM, CHAR(9), MPOOL)
            
            CASE INDEX(ARG, "@MPOOL", 1)
               IDX = INDEX(ARG, "@MPOOL", 1) -1
               DAT = ARG[1,IDX]:MPOOL
            
            CASE ARG = "@DPOOL"
               DAT = CONVERT(@VM, CHAR(9), DPOOL)
            
            CASE INDEX(ARG, "@DPOOL", 1)
               IDX = INDEX(ARG, "@DPOOL", 1) -1
               DAT = ARG[1,IDX]:DPOOL
            
            CASE ARG[1,1] = "!"
               DAT = ARG[2,LEN(ARG)]
            
            CASE ARG[1,1] = "@"
               DAT = ""
            
            CASE 1
               IFDEF.SW = 0
$IFDEF isRT
               LOOKIN  = VPOOL
               LOCATE ARG IN LOOKIN SETTING VPOS THEN IFDEF.SW = 1
$ELSE
               LOOKIN  = VPOOL
               LOCATE(ARG, LOOKIN; VPOS) THEN  IFDEF.SW = 1
$ENDIF
               IF IFDEF.SW THEN
                  DAT = DPOOL<1,VPOS>
               END ELSE
                  DAT = ARG
               END
         END CASE
         ARR := DAT:@FM
      NEXT A
      ETX = TIME()
      DIFF= ETX - STX
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   >> SR.DV.SUBSTITUTIONS Finished in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      RETURN
   END

