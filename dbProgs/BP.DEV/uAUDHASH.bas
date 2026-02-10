      PROMPT ""
$INCLUDE I_Prologue
      *--------------------------------------------------------------
      * CDC from Hashed &AUDLOGn% files and output to uDELTA.LOG    *
      *     The &AUDLOGn& file is owned by a sys admin user so this *
      *     must be run by that user, as a PHANTOM job.             *
      *--------------------------------------------------------------
      INCMD = EREPLACE(TRIM(@SENTENCE), " ", @FM)
      IGNORED = "ignored."
      HANDLED = "handled."
      VERBOSE = 0
      PROP.KEY = "properties"
      PROP.EXT = ""
      LOCATE("uAUDHASH", INCMD; rfPOS) THEN
         FOR I = 1 TO rfPOS
            INCMD = DELETE(INCMD, 1)
         NEXT I
      END
      EOI = DCOUNT(INCMD, @FM)
      FOR I = 1 TO EOI STEP 2
         IF UPCASE(INCMD<I>) = '-P' THEN PROP.KEY = INCMD<I+1>; PROP.EXT = "[":PROP.KEY:"]"
         IF UPCASE(INCMD<I>) = '-V' THEN VERBOSE = 1
      NEXT I
      MEMORY.VARS(1) = "audman":PROP.EXT
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "---------------------------------------------------"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      LOG.MSG = "Starting uAUDHASH()"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      LOG.MSG = "---------------------------------------------------"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      *
      * -----------------------------------------------------------
      * M A I N      L O O P                                       
      * -----------------------------------------------------------
      logCNT    = 0     ;* how many logged during the loop         
      QFILE     = ""
      ERR       = ""
      LOOP
         LOGFILES  = ""
         GOSUB INITIALISE
         IF STOP.SW THEN
            LOG.MSG = "STOP switch is ON. Stopping now."
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            EXIT
         END
         IF LOGFILES = "" THEN
            FOR I = 1 TO uvMAX.LOGS
               LOGFILES<I> = "&AUDLOG":I:"&"
            NEXT I
         END ELSE
            LOGFILES = EREPLACE(LOGFILES, " ", @FM)
         END
         * -------------------------------------------------------
         * loop through assigned log files                       *
         * -------------------------------------------------------
         EOF = DCOUNT(LOGFILES, @FM)
         *
         FOR FN = 1 TO EOF
            pCTR = 0     ;* how many logged during the full process
            CLEARDATA
            LOGFILE = LOGFILES<FN>
            IF LOGFILE = "" THEN CONTINUE
            AL = LOGFILE[8,9] ;* "&AUDLOGx&" ... AL = "x&"
            AL = FIELD(AL, "&", 1)
            IF AL = "" THEN CONTINUE
            *
            LFL  = "uplqf_":TRIM(EREPLACE(LOGFILE, "&", " "))
            READ CHECK FROM VOC, LFL ELSE
               qREC = "Q":@FM:"UV":@FM:LOGFILE
               WRITE qREC ON VOC, LFL
               LOG.MSG = " Created Q-File to [UV] [":LOGFILE:"] as [":LFL:"]"
               CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
               qREC = ""
            END
            CALL SR.FILE.OPEN (ERR, LFL, QFILE)
            IF ERR THEN 
               LOG.MSG = " CANNOT open [":LFL:"]. Ignoring this file."
               CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
               CONTINUE
            END
            *
            CALL uLOGGER(0, LOG.KEY:" ")
            TT1  = TIME()
            LOG.MSG = "Process <":AL:">   ":LOGFILE
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
            T1   = TIME()
            LOG.MSG = "  .) Suspend Log"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            GOSUB SUSPENDLOG
            T2   = TIME()
            IF VERBOSE THEN CALL uLOGGER(0, LOG.KEY:"   SUSPENDLOG took .... ":(T2-T1))
            * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
            T1   = TIME()
            LOG.MSG = "  .) Obtain CDC events"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            GOSUB PROCESS
            T2   = TIME()
            IF VERBOSE THEN CALL uLOGGER(0, LOG.KEY:"   PROCESS    took .... ":(T2-T1))
            * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
            T1   = TIME()
            LOG.MSG = "  .) Clean Log"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            GOSUB CLEANLOG
            T2   = TIME()
            IF VERBOSE THEN CALL uLOGGER(0, LOG.KEY:"   CLEANLOG   took .... ":(T2-T1))
            T1   = TIME()
            * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
            T1   = TIME()
            LOG.MSG = "  .) Resume Log"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            GOSUB RESUMELOG
            T2   = TIME()
            IF VERBOSE THEN CALL uLOGGER(0, LOG.KEY:"   RESUMELOG  took .... ":(T2-T1))
            * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
            TT2  = TIME()
            CALL uLOGGER(0, LOG.KEY:"   Done in ............ ":(TT2 - TT1):" seconds")
         NEXT FN
         IF pCTR=0 THEN
STOP ;* debug ONLY - not in product code-base !!
            LOG.MSG = " HEARTBEAT: No activity - waiting for database events "
            CRT LOG.MSG
            CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
            RQM
            READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW = ""
            IF STOP.SW THEN
               LOG.MSG = "STOP SWITCH is turned on in ":PROP.KEY
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               STOP
            END
         END
         RQM
      REPEAT
      * -----------------------------------------------------------
      * Only stops looping when BP.UPL STOP = stop
      * -----------------------------------------------------------
      CLOSE
      CLEAR
      STOP
      * -----------------------------------------------------------
SUSPENDLOG:
      CMD = EREPLACE(SUSPEND, "@", AL)
      CLEARDATA
      EXECUTE CMD, IN < SUDOPASSWD, OUT > MSG.REPLY
      IF INDEX(MSG.REPLY, " successful", 1) = 0 AND MSG.REPLY # "" THEN 
         CRT "ERROR: ":MSG.REPLY
         CALL uLOGGER(0, LOG.KEY:"  > ":MSG.REPLY)
      END
      RETURN
      * ------------------------------------------------------------
CLEANLOG:
      CMD = EREPLACE(CLEANUP, "@", AL)
      CLEARDATA
      EXECUTE CMD, IN < SUDOPASSWD, OUT > MSG.REPLY
      * don't log the output - if there are no deltas, the cleanlog says "abort"
      RETURN
      * ------------------------------------------------------------
RESUMELOG:
      CMD = EREPLACE(RESUME, "@", AL)
      CLEARDATA
      EXECUTE CMD, IN < SUDOPASSWD, OUT > MSG.REPLY
      IF INDEX(MSG.REPLY, " successful", 1) = 0 AND MSG.REPLY # "" THEN 
         CRT "ERROR: ":MSG.REPLY
         CALL uLOGGER(0, LOG.KEY:"  > ":MSG.REPLY)
      END
      RETURN
      * -----------------------------------------------------------
PROCESS:
      SELECT QFILE      ;* the AUDLOG file
      rnCTR=0     ;* Readnext counter
      lCTR=0
***   logCNT=0
      LOOP
         READNEXT LOGITEM ELSE EXIT
         lCTR+=1
         rnCTR+=1
         *
         * lCTR is here to accurately tell how many audlog items were checked.
         *
         READ TREC FROM QFILE, LOGITEM ELSE CONTINUE
         *
         OK.FLAG = 1
         EVT = TREC<1>   ; IF EVT="" THEN OK.FLAG = 0
         IF EVT#WRITE.EVT  THEN OK.FLAG = 0
         ACT = TREC<4>   ; IF ACT="" THEN OK.FLAG = 0
         FIL = TREC<7>   ; IF FIL="" THEN OK.FLAG = 0
         IID = TREC<8>   ; IF IID="" THEN OK.FLAG = 0
         IF NOT(OK.FLAG) THEN CONTINUE
         *
         IF INDEX(FIL, SLASH, 1) THEN 
            TMP = EREPLACE(FIL, SLASH, @FM)
            FIL = TMP<DCOUNT(TMP, @FM)>
            TMP = ""
         END
         IF TRIM(FIL) = "" THEN CONTINUE
         *
         IF INDEX(ACT, SLASH, 1) THEN
            TMP = EREPLACE(ACT, SLASH, @FM)
            ACT = TMP<DCOUNT(TMP, @FM)>
            TMP = ""
         END
         IF TRIM(ACT) = "" THEN CONTINUE
         *
         THIS.EVENT = EVT:"  [":ACT:"]   [":FIL:"]   [":IID:"]"
         *
         EVENT.LOCN = ACT:" ":FIL
         LOCATE(EVENT.LOCN, REGISTER; rfPOS) ELSE 
            EVENT.LOCN = ACT:" *"
            LOCATE(EVENT.LOCN, REGISTER; rfPOS) ELSE CONTINUE
         END
         IF INDEX(FIL, ACT, 1) THEN 
            LOG.MSG = " Strange occurance of account [":ACT:"] in file [":FIL:"] event ignored."
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         END
         *
         QFL = "uplqf_":ACT:"_":FIL
         QDATA=""
         LOCATE(QFL, FNAMES; rfPOS) THEN
            * already opened.
            QDATA = FHANDLES(rfPOS)
         END ELSE
            REC = "Q":@FM:ACT:@FM:FIL
            WRITE REC ON VOC, QFL
            CALL SR.FILE.OPEN (ERR, QFL, QDATA)
            IF ERR#"" THEN CONTINUE
         END
         *
         READ RECORD FROM QDATA, IID ELSE 
            CALL uLOGGER(0, LOG.KEY:THIS.EVENT:"   ignored - id not on file")
            CONTINUE
         END
         *
         * -------------------------------------------------
         * Only take what is required. Check DICT uDELTA.LOG
         * for product types, dates, etc.                   
         * BEST use is in a selected list of records: INLIST
         * -------------------------------------------------
         * 
         PASSED = 0
         CTL.ID = SEP:ACT:SEP:FIL
         LOCATE(CTL.ID, MGR.EVENTS; CTL.POS) THEN
            INLIST     = MGR.LISTS<CTL.POS>
            FILTER     = MGR.FILTER<CTL.POS>
            STARTSWITH = MGR.STARTSWITH<CTL.POS>
            ENDSWITH   = MGR.ENDSWITH<CTL.POS>
            CONTAINS   = MGR.CONTAINS<CTL.POS>
            *
            BEGIN CASE
               CASE INLIST # ""
                  * If IID is in a selected list, then NO other checks required !!
                  LOCATE(IID, CHK; FND) THEN
                     PASSED = 1
                  END ELSE
                     IF VERBOSE THEN
                        CALL uLOGGER(0, LOG.KEY:THIS.EVENT:"   ignored - id not in focus list")
                     END
                     CONTINUE
                   END
                CASE 1
                  AV = FIELD(FILTER, ",", 1)
                  MV = FIELD(FILTER, ",", 2)
                  SV = FIELD(FILTER, ",", 3)
                  IF AV = 0 THEN
                     FILTER = IID
                  END ELSE
                     FILTER = RECORD<AV, MV, SV>
                  END
                  * PASSED is now an OR construct - if any is true, then pass it
                  IF STARTSWITH # "" THEN
                     CHK = FILTER[1, LEN(STARTSWITH)]
                     IF CHK = STARTSWITH THEN PASSED = 1
                  END
                  IF NOT(PASSED) AND ENDSWITH # "" THEN
                     CHK = FILTER[(LEN(FILTER) - LEN(ENDSWITH)) + 1, LEN(ENDSWITH)]
                     IF CHK = ENDSWITH THEN PASSED = 1
                  END
                  IF NOT(PASSED) AND CONTAINS # "" THEN
                     IF INDEX(FILTER, CONTAINS, 1) THEN PASSED = 1
                  END
                  *
                  IF NOT(PASSED) THEN
                     IF VERBOSE THEN
                        CALL uLOGGER(0, LOG.KEY:THIS.EVENT:"   ignored - not in filters")
                     END
                     CONTINUE
                  END
            END CASE
         END ELSE
            PASSED = 1
            IF VERBOSE THEN
               CALL uLOGGER(0, LOG.KEY:THIS.EVENT:"   processing - unfiltered")
            END
         END
         * 
         * -------------------------------------------------
         IF PASSED THEN
            * Audit Event Service
            CALL uAES(ERR, LOGITEM, ACT, FIL, IID, RECORD, EXTN)
            IF ERR THEN 
               CALL uLOGGER(0, LOG.KEY:ERR)
               CRT ERR
               STOP
            END
            pCTR+=1
            logCNT+=1
         END
         IF rnCTR >= PAUSE THEN
            IF VERBOSE THEN
                LOG.MSG = "     > Checked ":lCTR:" records and logged ":logCNT:" events."
                CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            END
            rnCTR=0
            READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW = ""
            IF STOP.SW THEN
               LOG.MSG = "STOP SWITCH is turned on in ":PROP.KEY
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               STOP
            END
         END
      REPEAT
      RQM
      LOG.MSG = "     > Checked ":lCTR:" records and logged ":logCNT:" events."
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      RETURN
      * -----------------------------------------------------------
GET..PROPERTY:
      * Find the value of PROPERTY in PROCESS.CONTROL
      PROP.POS = INDEX(UPCASE(PROCESS.CONTROL), UPCASE(PROPERTY:"="), 1)
      TMP = PROCESS.CONTROL[PROP.POS, LEN(PROCESS.CONTROL)]
      RETURN.VAR = FIELD(TMP<1>, "=", 2)
      RETURN
      * -----------------------------------------------------------
INITIALISE:
      PROG = "uAUDHASH"
      IF UNASSIGNED(VOC) THEN
         CALL SR.FILE.OPEN (ERR, "VOC", VOC); IF ERR THEN STOP
         CALL SR.FILE.OPEN (ERR, "BP.UPL", BP.UPL); IF ERR THEN STOP
         CALL SR.FILE.OPEN (ERR, "DICT uDELTA.LOG", uDELTA.DICT); IF ERR THEN STOP
         CALL SR.FILE.OPEN (ERR, "&SAVEDLISTS&", SL); IF ERR THEN STOP
      END
      *
      * --------------------------------
      * REGISTER of files to stream     
      * {account} {file}                
      * {account} {file}                
      * etc....                         
      * --------------------------------
      *
      READ REGISTER FROM uDELTA.DICT, "@REGISTER" ELSE REGISTER = ""
      IF TRIM(REGISTER) = "" THEN 
         LOG.MSG = "no files are registered"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         STOP
      END
      REGISTER = EREPLACE(REGISTER, ",", "@FM")  ;* ensure that the LOCATE works
      READ PROPS FROM BP.UPL, PROP.KEY ELSE 
         LOG.MSG = "properties file - '":PROP.KEY:"' is missing from BP.UPL"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         STOP
      END
      uvHOME.DIR  =  ""
      uvMAX.LOGS  =  2
      PAUSE       = 250
      RQMCNT      = 5
      SUDOPASSWD  = ""  ;* not needed if run as uvadm or admin user
      EOI = DCOUNT(PROPS, @FM)
      FOR I = 1 TO EOI
         KEY = FIELD(PROPS<I>, "=", 1)
         IF UPCASE(KEY) = "AUDLOG"  THEN LOGFILES   = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "UVHOME"  THEN uvHOME.DIR = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "MAXLOG"  THEN uvMAX.LOGS = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "PAUSE"   THEN      PAUSE = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "RQMCNT"  THEN     RQMCNT = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "SUDO"    THEN SUDOPASSWD = FIELD(PROPS<I>, "=", 2)
      NEXT I
      READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW = ""
      IF UPCASE(STOP.SW) = "STOP" THEN STOP.SW = 1 ELSE STOP.SW = 0
      IF (uvHOME.DIR="") THEN
         LOG.MSG = "'uvhome' is missing from BP.UPL properties"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         STOP
      END
      IF NOT(NUM(uvMAX.LOGS)) OR uvMAX.LOGS < 2 THEN
         LOG.MSG = "'maxlog' is missing or incorrectly set in BP.UPL properties"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         STOP
      END
      * ----------------------------------------------------------------------------------
      READ CHK FROM VOC, "uvHOME" ELSE
         WRITE "Q":@FM:"UV":@FM:"VOC" ON VOC, "uvHOME"
      END
      *
      READ PROCESS.CONTROL FROM uDELTA.DICT, "@CONTROL" THEN
         PROPERTY = "sep" ; GOSUB GET..PROPERTY ; SEP = RETURN.VAR
      END ELSE
         SEP = "@"
      END
      * ----------------------------------------------------------------------------------
      MGR.EVENTS     = ""
      MGR.FILTER     = ""
      MGR.STARTSWITH = ""
      MGR.ENDSWITH   = ""
      MGR.CONTAINS   = ""
      MGR.LISTS      = ""
      COMMA          = ","
      * --------------------------------------------------
      * Get ALL control details from DICT uDELTA.LOG and  
      * store them in a set of master arrays for use.     
      * e.g. DICT uDELTA.LOG @RFUEL@MYFILE                
      * --------------------------------------------------
      EXE = "SSELECT DICT uDELTA.LOG LIKE ":SEP:"...":SEP:"..."
      EXECUTE EXE CAPTURING JUNK
      *
      LOOP
         READNEXT CTL.ID ELSE EXIT
         READ AUD.CTRL FROM uDELTA.DICT, CTL.ID ELSE CONTINUE
         * --------------------------------------------------
         ERR = ""
         FILTER.CHK = AUD.CTRL<1>:AUD.CTRL<2>:AUD.CTRL<3>:AUD.CTRL<4>:AUD.CTRL<5>
         IF FILTER.CHK # "" THEN
            AMS.CHK = AUD.CTRL<1>
            AV = FIELD(AMS.CHK, ",", 1)
            IF AV = "" THEN AV = 0
            IF NOT(NUM(AV)) THEN ERR := "  AV is non-numeric"
            MV = FIELD(AMS.CHK, ",", 2)
            IF MV = "" THEN MV = 0
            IF NOT(NUM(MV)) THEN ERR := "  MV is non-numeric"
            SV = FIELD(AMS.CHK, ",", 3)
            IF SV = "" THEN SV = 0
            IF NOT(NUM(SV)) THEN ERR := "  SV is non-numeric"
            IF ERR # "" THEN
                 CALL uLOGGER(0, LOG.KEY:"FATAL: ":ERR:" [":AMS.CHK:"]")
                 STOP
             END
             AMS.CHK = AV:COMMA:MV:COMMA:SV
             *
             MGR.EVENTS     := CTL.ID:@FM
             MGR.FILTER     := AMS.CHK:@FM
             MGR.STARTSWITH := AUD.CTRL<2>:@FM
             MGR.ENDSWITH   := AUD.CTRL<3>:@FM
             MGR.CONTAINS   := AUD.CTRL<4>:@FM
             *
             READ INLIST FROM SL, AUD.CTRL<5> THEN
                INLIST = EREPLACE(INLIST, @FM, @VM)
             END ELSE
                INLIST = ""
             END
             MGR.LISTS      := INLIST:@FM
         END
         * --------------------------------------------------
      REPEAT
      EXE  = ""
      JUNK = ""
      * ----------------------------------------------------------------------------------
      WRITE.EVT = "DAT.BASIC.WRITE"
      EXTN = '.ulog'
      SLASH= "/"
      *************************************************************************************
      ** Notes: changed process to now run as a non-admin user who IS in the sudo group  !!
      **        it is safer to keep the user's password in BP.UPL 'properties' than root !!
      *************************************************************************************
      SUSPEND = 'sh -c"sudo ':uvHOME.DIR:'bin/audman -suspendlog @"'
      CLEANUP = 'sh -c"sudo ':uvHOME.DIR:'bin/audman -clearlog @ -force"'
      RESUME  = 'sh -c"sudo ':uvHOME.DIR:'bin/audman -resumelog @"'
      *
      RETURN
   END
