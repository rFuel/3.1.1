$INCLUDE I_Prologue
*************************************************************************
***   Program: uPREP                                                     
***   Usage  : uPREP {file} {correl} {aStep} {RunType} {uList} {queues}  
***   -------------------------------------------------------------------
***   RunType=REFRESH / FULL                                             
***   -  *.TAKE and *.LOADED are cleared in rFuel                        
***   -  Select base file and load everything                            
***   -  Take an MD5 hash and store in *.TAKE (ignore loaded)            
***   -------------------------------------------------------------------
***   RunType=INCR                                                       
***   -  *.TAKE and *.LOADED are built - do not clear them.
***   -  SELECT base file                                                
***      -  Read every record and take an MD5 hash of the current record 
***      -  Compare this hash with the record in *.LOADED                
***      -  If they are the same - skip the record --> not a delta       
***      -  If they are different- take the record -->  IS a delta       
***                                                                      
*************************************************************************
      *
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      IF DBT#"UV" AND DBT#"UD" THEN
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
      LOG.LEVEL=0
      OPEN "BP.UPL" TO BP.UPL THEN
         READ RQM.LIMIT FROM BP.UPL, "RQM" ELSE RQM.LIMIT = 99999
      END ELSE
         MSG = "Cannot find BP.UPL.  ABORTING"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG); PRINT MSG
         STOP
      END
      MSG = "uPREP Started **********************************************"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG); PRINT MSG
      SLIST="THROW-ERROR"
      IF DBT="UV" THEN SLIST = "&SAVEDLISTS&"
      IF DBT="UD" THEN SLIST = "SAVEDLISTS"
      IF DBT="RT" THEN SLIST = "POINTER-FILE"
      OPEN SLIST TO SL ELSE
         MSG = SLIST:" cannot be accessed!"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG); PRINT MSG
         STOP
      END
$IFDEF isRT
      CMD = SENTENCE()
      CMD = CONVERT(CMD, " ", @FM)
$ELSE
      CMD = TRIM(@SENTENCE)
      CONVERT " " TO @FM IN CMD
$ENDIF
      uLST=""; RTYP=""; SLEEPER=0
      LOCATE("uPREP", CMD; POS) THEN
         FILE = CMD<POS+1>                ; * e.g. CLIENT_DATA_12345
         IF FILE="DICT" THEN POS+=1; FILE = "DICT ":CMD<POS+1>
         CORR = CMD<POS+2>                ; * e.g. uWhse_CLIENT_DATA_12345
         WBLK = CMD<POS+3>                ; * e.g. 9999
         RTYP = CMD<POS+4>                ; * e.g. FULL / INCR
         uLST = CMD<POS+5>                ; * e.g. CLIENT_12
         
         QUEUES = CMD<POS+6>              ; * ##
         Q.NBR= 0                         ; * ##
         IF NOT(NUM(QUEUES)) OR QUEUES="" THEN QUEUES = 0
         READV CHK FROM SL, uLST, 1 ELSE uLST = ""
         
         FILE.AND.ACCOUNT = FILE
         IF COUNT(FILE, "_") = 2 THEN
            * E.G. upl_CUSTOMER_1272
            FILE.AND.ACCOUNT = FILE[1,INDEX(FILE, "_", 2)-1]
         END
         LNAME=CORR
         LNAME := "_"
         IF FILE[LEN(FILE)-4,5]=".TAKE" THEN
            FILE=FILE[1,LEN(FILE)-5]
            IS.TAKE=1
         END ELSE
            IS.TAKE=0
         END
         *
         IF RTYP="" THEN RTYP="FULL"
         BEGIN CASE
            CASE RTYP="INCR"  ; INCR=1
            CASE 1            ; INCR=0
         END CASE
         aFROM=0 ; aTO=0
         LISTIO = ""
         IGNORE.THE.DATA=0
         IF CORR[1,8] = "FetchKey" THEN IGNORE.THE.DATA = 1
         LDD=0; TAK=0
         PID = FIELD(FILE, "_", DCOUNT(FILE, "_"))
         *
         IF FILE[1,4]="DICT" THEN uFILE=EREPLACE(FILE, " ", "_") ELSE uFILE=FILE
         * ---------
$IFDEF isRT
         VFILE = "MD"
$ELSE
         VFILE = "VOC"
$ENDIF
         CALL SR.FILE.OPEN(ERR, VFILE, VOC)
         IF ERR # "" THEN
            MSG = "FATAL: cannot open ":VFILE
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            STOP
         END
         zFILE = FILE
         IF FILE[1,4] = "DICT" THEN zFILE = FILE[6,LEN(FILE)]
         READ VREC FROM VOC, zFILE THEN
            IF UPCASE(VREC<1>[1,1]) = "Q" THEN
               uFILE = TRIM(VREC<3>):"_":TRIM(VREC<2>)
               IF FILE[1,4] = "DICT" THEN uFILE = "DICT_":uFILE
            END
         END
         * ---------
         OPEN FILE TO IOFILE THEN
            OPEN uFILE:".LOADED" TO LOADED THEN LDD=1
            OPEN uFILE:".TAKE"   TO TAKE   THEN TAK=1
            GOSUB GET..LISTIO
            IF uLST = "" THEN
               IF (IS.TAKE) THEN
                  SELECT TAKE
               END ELSE
                  SELECT IOFILE
               END
            END ELSE
$IFDEF isRT
               EXE = "GET.LIST ":uLST
$ELSE
               EXE = "FORM.LIST ":SLIST:" ":uLST
$ENDIF
               EXECUTE EXE CAPTURING JUNK
            END
            TAK.LIST=""; TAK.CNT=0
            LX = 0 ; LIST="" ; PREV = "" ; FM=""; IDx=1
            CNT= 0 ; FND =0; PCNT =0 ; SCNT =0
            LOOP
               SCNT += 1
               IF SCNT > 10 THEN
                  STOP.SW = 0
                  GOSUB CHECK..STOP
                  IF STOP.SW THEN CLEARSELECT; EXIT
                  SCNT =0
               END
               READNEXT ID ELSE EXIT
               IF ID = "" THEN CONTINUE
               IF ID="[<END>]" THEN EXIT
               IF NOT(IGNORE.THE.DATA) THEN
                  * --------------------------------------------------------
                  *  Get the DELTAs only
                  * --------------------------------------------------------
                  IF INCR THEN PCNT+=1; CNT+=1
                  PREV=""
                  IF LDD THEN READ PREV FROM LOADED, ID ELSE PREV=""
                  READ REC FROM IOFILE, ID THEN
                     HASH=""
                     IF REC # "" THEN
                        CALL uMD5(HASH, REC)
                        IF HASH = "" THEN 
                           CALL uLOGGER(LOG.LEVEL, LOG.KEY:FILE:" ":ID:" cannot be HASHED!")
                           CONTINUE 
                        END
                     END
                     *
                     IF INCR AND PREV<1>=HASH THEN 
                        * if an application creates an empty record, this will not see it as a delta.
                        CONTINUE
                     END ELSE
                        IF TAK THEN 
                           TAK.LIST<-1> = ID:@VM:HASH
                           TAK.CNT += 1
                           IF TAK.CNT => 1000 THEN GOSUB FLUSH..TAKE
                           FND += 1
                        END
                     END
                  END
               END
               *
               IF LEN(ID) > 0 THEN LIST := FM:ID ; FM = @FM
               LX += 1
               IF LX => WBLK THEN
                  STOP.SW = 0
                  GOSUB CHECK..STOP
                  IF STOP.SW THEN CLEARSELECT; EXIT
                  CALL uLOGGER(LOG.LEVEL, LOG.KEY:"uPREP  >>>>>>>> Finised ":USE.THIS:" with ":LX:" records")
                  GOSUB WRITE..LIST
                  LIST="" ; LX=0
                  GOSUB GET..LISTIO
               END
               SLEEPER+=1; IF SLEEPER > RQM.LIMIT THEN GOSUB SNORE
            REPEAT
            IF TAK THEN GOSUB FLUSH..TAKE
            LIST := @FM:"[<END>]":@FM:"[<END>]"
            GOSUB WRITE..LIST
            IF INCR THEN
               MSG = " *********** delta process finished for ":FILE
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
               MSG = " Checked ":CNT:" records, found ":FND:" deltas"
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            END
         END ELSE
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:FILE:" cannot be accessed!")
         END
      END
      IF TAK THEN GOSUB FLUSH..TAKE
      LIST = @FM:"[<EOP>]"
      GOSUB WRITE..LIST
      RELEASE
$IFNDEF isRT
      CLOSE
$ENDIF
      MSG = "uPREP Finished *********************************************"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      PRINT
      PRINT MSG
      PRINT
      PRINT '-------------------------[ END ] --------------------------'
      STOP
*************************************************************************
CHECK..STOP:
      STOP.SW =0
      READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW=""
      IF STOP.SW="stop" THEN
         LMSG = FILE:"         ---------------------"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:LMSG) 
         LMSG = FILE:" Stopped - STOP switch set ON!":@FM:STR('*',70)
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:LMSG) 
         LMSG = FILE:"         ---------------------"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:LMSG) 
         STOP.SW = 1
      END
      RETURN
*************************************************************************
SNORE:
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:FILE:" *************** PAUSE ***************")
      RQM; RQM; SLEEP 2; RQM; SLEEPER = 0
      RETURN
*************************************************************************
WRITE..LIST:
      ANS = ""
      BEGIN CASE
         CASE DBT="UV"
            CALL uWRITE_UV(ANS, DBT, LIST, LISTIO)
         CASE DBT="UD"
            CALL uWRITE_UD(ANS, DBT, LIST, LISTIO)
         CASE DBT="RT"
            WRITESEQ LIST ON LISTIO ELSE ANS = " WRITEBLK failed"
      END CASE
      IF ANS # "" THEN
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"FATAL error:: ":ANS)
         STOP
      END ELSE 
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" >> Updated: ":USE.THIS)
      END
      RETURN
      *
      ******************************************************************
      *
GET..LISTIO:
      IF FILEINFO(LISTIO,0) THEN CLOSESEQ LISTIO
      IF aFROM=0 THEN
         aFROM=1
         aTO=WBLK
      END ELSE
         aFROM = aTO + 1
         aTO = aFROM + WBLK
      END
      USE.THIS = LNAME:aFROM
      OPEN SLIST TO JUNKIO THEN
         WRITE "" ON JUNKIO, USE.THIS
         CLOSE JUNKIO
      END ELSE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"FATAL - ":SLIST:" is missing")
         STOP
      END
      OPENSEQ SLIST,USE.THIS TO LISTIO ELSE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:SLIST:",":USE.THIS:' open failure ')
         STOP
      END
      MSG = "    uPREP  >> creating block ":SLIST:",":USE.THIS
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      FM = ""
      RETURN
      *
      ******************************************************************
      *
FLUSH..TAKE:
      EOTI = DCOUNT(TAK.LIST, @FM)
      FOR TI = 1 TO EOTI
         TKID= TAK.LIST<TI,1>
         TKREC=TAK.LIST<TI,2>:@FM:TIMEDATE()
         WRITE TKREC ON TAKE, TKID
      NEXT TI
      TAK.LIST = ""
      TAK.CNT = 0
      RETURN
   END
