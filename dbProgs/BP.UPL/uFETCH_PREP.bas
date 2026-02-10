      SUBROUTINE uFETCH_PREP (ANS, RUNTYPE, FILE, CORRELATION, aSTEP, DACCT)
$INCLUDE I_Prologue
      *
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      IF DBT#"UV" AND DBT#"UD" AND DBT#"RT" THEN
         UPL.LOGGING = 0
         INF.LOGGING = 0
         MAT sockPROPS = ""
         CALL SR.OPEN.CREATE(ERR, "BP.UPL", "19", BP.UPL)
         IF ERR THEN
            DBT = "UV"
         END ELSE
            READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
            READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
            MATPARSE sockPROPS FROM PARAMS
            pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = pAns
            pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
            IF PARAMS="" THEN
               PARAMS<1> = UPL.LOGGING
               PARAMS<2> = INF.LOGGING
            END
            WRITE DBT ON BP.UPL, "DBT"
            WRITE PARAMS ON BP.UPL, "properties"
         END
      END
      *
      CHKTYPE = "FetchKey"
      IF ANS # "" THEN
         PREPARED.LIST = ANS
      END ELSE
         PREPARED.LIST = ""
      END
      LOG.LEVEL = 0
      IF RUNTYPE = "" THEN RUNTYPE = "FULL"
      ANS = ""
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:"uFETCH_PREP started for ":FILE)
$IFDEF isRT
      VFILE = "MD"
$ELSE
      VFILE = "VOC"
$ENDIF
      OPEN VFILE TO VOC ELSE STOP "No VOC file"
      TRY=0
      IF FILE[1,4] = "DICT" THEN FILE = FILE[6,LEN(FILE)]; DCT=1 ELSE DCT=0
      QF.flg = 0
      LOOP
         TRY+=1
         IF TRY > 2 THEN
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:"<<FATAL>> Cannot open or setup for ":FILE)
            JUNK=""; GO RETN
         END
         READ REC FROM VOC, FILE THEN
            EXIT
         END ELSE
            IF DBT="UV" THEN REC = "Q":@FM:DACCT:@FM:FILE
            IF DBT="RT" THEN REC = "Q":@FM:DACCT:@FM:FILE
            IF DBT="UD" THEN REC = "F":@FM:DACCT:FILE:@FM:DACCT:"D_":FILE
            WRITE REC ON VOC, FILE
            WRITE REC ON VOC, FILE:".QF"
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:"Q-pointers created for ":FILE)
         END
      REPEAT
      EXTN = ".TAKE"    ; GOSUB CHECK..FILE
      EXTN = ".LOADED"  ; GOSUB CHECK..FILE
      IF DBT="UV" THEN SL = "&SAVEDLISTS&"
      IF DBT="UD" THEN SL = "SAVEDLISTS"
      IF DBT="RT" THEN SL = "POINTER-FILE"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:"Clearing old select-lists")
      IF DCT THEN CORRELATION = EREPLACE(CORRELATION, " ", "_")
$IFDEF isRT
      EXE = "SELECT ":SL:' = "':DACCT:"*L*":CORRELATION:FILE:']"' 
$ELSE
      EXE = "SELECT ":SL:" WITH @ID LIKE ":CORRELATION:"..."
$ENDIF
      *
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:EXE)
      EXECUTE EXE CAPTURING JUNK
      DEL = "DELETE ":SL:" "
      LOOP
         READNEXT ID ELSE EXIT
         EXECUTE DEL:ID CAPTURING JUNK
      REPEAT
      * --------------------------------------------------------
      IF PREPARED.LIST # "" THEN
         ANS = PREPARED.LIST
         PREPARED.LIST = ""
         ANS = EREPLACE(ANS, "<tm>", @FM)
         PREPARED.LIST = CORRELATION:"_ufPREP"
$IFDEF isRT
         EXE = "DELETE ":SL:" ":DACCT:"*L*":PREPARED.LIST 
$ELSE
         CMD = "DELETE.LIST ":PREPARED.LIST
$ENDIF
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"Executing: ":CMD)
         EXECUTE CMD CAPTURING uvReply
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" reply : ":EREPLACE(uvReply, @FM , " "))
         * -----------------------------------------------------
         CMD = ANS<1>                  ;*  SELECT command
         FIRST.WORD = UPCASE(FIELD(CMD:" ", " ", 1))
         IF INDEX(FIRST.WORD, "SELECT", 1) THEN
            NULL
         END ELSE
            IF FIRST.WORD = "FORM.LIST" THEN
               NULL
            END ELSE
               CMD = "GET.LIST ":CMD
            END
         END
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"Executing: ":CMD)
         EXECUTE CMD RTNLIST SEL.LIST0 CAPTURING uvReply
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" reply : ":EREPLACE(uvReply, @FM , " "))
         * -----------------------------------------------------
         CMD = ANS<2>                  ;* NSELECT command
         IF CMD # "" THEN
            IF UPCASE(FIELD(CMD, " ", 1)) # "NSELECT" THEN CMD = "GET.LIST ":CMD
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:"Executing: ":CMD)
            EXECUTE CMD PASSLIST SEL.LIST0 RTNLIST SEL.LIST1 CAPTURING uvReply
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:" reply : ":EREPLACE(uvReply, @FM , " "))
            CMD = "SAVE.LIST ":PREPARED.LIST
            EXECUTE CMD PASSLIST SEL.LIST1 CAPTURING uvReply
         END ELSE
            CMD = "SAVE.LIST ":PREPARED.LIST
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:"Executing: ":CMD)
            EXECUTE CMD PASSLIST SEL.LIST0 CAPTURING uvReply
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:" reply : ":EREPLACE(uvReply, @FM , " "))
         END
      END
      * --------------------------------------------------------
      IF DCT THEN pFILE = "DICT ":FILE ELSE pFILE = FILE
$IFDEF isRT
      CMD = "uPREP ":pFILE:" ":CORRELATION:" ":aSTEP:" ":RUNTYPE:" ":PREPARED.LIST
$ELSE
      CMD = "PHANTOM uPREP ":pFILE:" ":CORRELATION:" ":aSTEP:" ":RUNTYPE:" ":PREPARED.LIST
$ENDIF
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" >> ":CMD)
      EXECUTE CMD CAPTURING JUNK
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" >> ":EREPLACE(JUNK, @FM , ". "))
RETN:
      CLOSE VOC
      IF UPCASE(RUNTYPE) # "UREST" THEN
         RQM ; RQM ; SLEEP 3
      END
      RETURN
**************************************************************************************
CHECK..FILE:
      IF FILE[LEN(FILE)-4,5]=".TAKE" THEN RETURN
      * ---------
      useFILE = FILE
      PSX = INDEX(FILE, "_":DACCT:"_", 1)
***   IF PSX THEN useFILE = FIELD(FILE, "_":DACCT, 1):"_":DACCT
      IF PSX THEN useFILE = FILE[1,(PSX-1)]:"_":DACCT
      IF useFILE[1,4] = "upl_" THEN useFILE = useFILE[5, LEN(useFILE)]
      * ---------
      IF DCT THEN useFILE = "DICT_":useFILE
      OPEN useFILE:EXTN TO JUNKIO ELSE
         IF DBT = "UV" THEN
            EXE = "CREATE.FILE ":useFILE:EXTN:" DYNAMIC"
         END ELSE
            EXE = "CREATE.FILE ":useFILE:EXTN:" 919,3 DYNAMIC KEYDATA"
         END
         EXECUTE EXE CAPTURING JUNK
         OPEN useFILE:EXTN TO JUNKIO ELSE
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:"****************************************")
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:useFILE:EXTN:" cannot be created!")
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:JUNK)
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:"****************************************")
            STOP
         END
      END
      CLOSE JUNKIO
      RETURN
   END
