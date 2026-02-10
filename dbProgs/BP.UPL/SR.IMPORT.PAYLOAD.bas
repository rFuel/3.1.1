      SUBROUTINE SR.IMPORT.PAYLOAD (MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * example ucat-x-cImporter    Vanilla Data Importer       
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU REPLY   TO IN.STRINGS(1)
      EQU EIOID   TO IN.STRINGS(2)   ;* uEIO.ID ... e.g. PersonApplicant
      EQU VPOOL   TO IN.STRINGS(3)
      EQU MPOOL   TO IN.STRINGS(4)
      EQU DPOOL   TO IN.STRINGS(5)
      * --------------------------------------------------------
      PRECISION 9
      FATAL.ERR=0
      STX = TIME()
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   >> SR.IMPORT.PAYLOAD Started for [":EIOID:"]"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      
      CALL SR.OPEN.CREATE(FATAL.ERR, "uEIO", "", EIOFILE)
      IF FATAL.ERR THEN
         RTN.CODE = 500
         RTN.MSG = "SR.IMPORT.PAYLOAD cannot open/create uEIO"
         GOTO ERROR..CONDITION
      END
      * --------------------------------------------------------
      * Obtain the file name to dump imported data into         
      * --------------------------------------------------------
      FILENAME = EIOID     ;* e.g. PersonApplicant
      PARENT   = EIOID     ;*
      LOOP
         READ REC FROM EIOFILE, PARENT THEN
            PARENT = REC<1>      ;* e.g. CAL
            IF PARENT # "" THEN
               FILENAME = PARENT:".":FILENAME   ;* e.g. CAL.PersonApplicant
            END ELSE
               EXIT
            END
         END ELSE
            RTN.CODE = 500
            RTN.MSG = "SR.IMPORT.PAYLOAD cannot find [":PARENT:"] in uEIO"
            GOTO ERROR..CONDITION
         END
      REPEAT
      * --------------------------------------------------------
      * Dump the data according to its layout                   
      * --------------------------------------------------------
      DIM CALL.STRINGS(20)         ; MAT CALL.STRINGS = ""
      CALL.STRINGS(2) = FILENAME   ;* e.g. LIXI.CAL.PersonApplicant
      CALL.STRINGS(3) = VPOOL      ;* all variables for the payload
      CALL.STRINGS(4) = MPOOL      ;* all the database mapping spec's.
      CALL.STRINGS(5) = DPOOL      ;* all data from the payload
      CALL.STRINGS(6) = 1          ;* @true = an operation on a uEIO
      *............................;* @false= an operation on a uISO
      *
      CALL SR.WRITETOLAYOUT (MAT CALL.STRINGS)
      IF CALL.STRINGS(1) # "" THEN
         RTN.CODE = 500
         RTN.MSG = CALL.STRINGS(1)
         GOTO ERROR..CONDITION
      END
      * --------------------------------------------------------
      ETX = TIME()
      DIFF= ETX - STX
      LOG.MSG = "   >> SR.IMPORT.PAYLOAD Finished for [":EIOID:"] in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      RETURN
      * --------------------------------------------------------
ERROR..CONDITION:
      ETX = TIME()
      DIFF= ETX - STX
      LOG.MSG = "   >> SR.IMPORT.PAYLOAD Error with [":EIOID:"] finished in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      REPLY = RTN.CODE:" - ":RTN.MSG
      RETURN
   END
