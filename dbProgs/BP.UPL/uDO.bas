      PROMPT ""
$DEFINE isUV 1
      **********************************************************
      * Usage:                                                 *
      * uDO                   (compiles BP.UPL)                *
      * uDO MY.PROG           (compiles MY.PROG from BP.UPL)   *
      * uDO -F BP.HBS         (compiles BP.HBS)                *
      * uDO -F BP.HBS MY.PROG (compiles MY.PROG from BP.HBS)   *
      **********************************************************
      PROG = "uDO"
      OPEN "BP.UPL" TO BPDATA ELSE
         PRINT "You must CREATE-FILE BP.UPL {options} - see database guide"
         STOP
      END
      READ DBT FROM BPDATA, "DBT"      ELSE DBT = "UV"
      READ REG FROM BPDATA, "REGISTER" ELSE REG = ""
      * ---------------------------------------------------------
      *  ABSOLUTELY MUST have unique instance id - named commons
      * ---------------------------------------------------------
      UVMAX = 31
      PAD = "_"
      EXE = "SH -c'hostname'"
      EXECUTE EXE CAPTURING HOST
      HOST = EREPLACE(HOST, @FM, "")
      HOST = FIELD(HOST, ".", 1)
      HOST = EREPLACE(HOST, "-", PAD)
      HOST = EREPLACE(HOST, ".", PAD)
      EXE = 'SH -c"pwd -P"'         ;* physical (true) location
      EXECUTE EXE CAPTURING PWD
      PWD = EREPLACE(PWD, @FM     , "")
      PWD = EREPLACE(PWD, CHAR(10), "")
      PWD = EREPLACE(PWD, CHAR(13), "")
      PWD = EREPLACE(PWD, "/"     , PAD)
      IAM = OCONV(HOST:PWD, "MCL")
      LX  = LEN(IAM)
      IAM = EREPLACE(IAM, "_", PAD)
      IAM = EREPLACE(IAM, ".", PAD)
      IF LX > UVMAX THEN
         * this is a UV limitation on named-common names
         CRT "[":IAM:"]  is > ":UVMAX:" bytes."
         LOOP WHILE LX > UVMAX AND IAM # "" DO
            IAM = IAM[2,LEN(IAM)]
            LX = LEN(IAM)
         REPEAT
         IF IAM = "" THEN
            CRT "Instance ID could not be generated — check hostname & pwd -P."
            STOP
         END
         CRT "Using: [":IAM:"] instead !!"
      END
      IF REG = ""  THEN 
         REG = IAM                 ;* Fresh Install
         WRITE REG ON BPDATA, "REGISTER"
      END
      IF REG # IAM THEN
         CRT "[WARN]  ":@WHO:" has been MOVED."
         REG = IAM                 ;* Fresh Install
         WRITE REG ON BPDATA, "REGISTER"
      END
      INSTANCE.ID = IAM
      *---------------------------------------------------------
      CMD = TRIM(SENTENCE())
      CONVERT " " TO @FM IN CMD
      LOCATE(PROG, CMD; POS) ELSE STOP "Not a valid DO command"
      *
      EOL = DCOUNT(CMD, @FM) ; BP = "" ; KEEP = 0 ; DO.ALL = 0 ; ITEM = "" ; atHOME = 0
      POS += 1
      FOR L = POS TO EOL
         CHK = CMD<L>
         BEGIN CASE
            CASE CHK = "-H"   ; atHOME    = 1
            CASE CHK = "-F"   ; BP        = CMD<L+1> ; L+=1
            CASE CHK = "-K"   ; KEEP      = 1
            CASE CHK = "ALL"  ; DO.ALL    = 1
            CASE 1            ; ITEM<-1>  = CHK
         END CASE
      NEXT L
      *
      IF BP="" OR BP="BP.UPL" THEN 
         BP = "BP.UPL"
         BPIO = BPDATA
      END ELSE
         OPEN BP TO BPIO ELSE 
$IFDEF isRT
            EXECUTE "CREATE-FILE ":BP:" 1,1 7,1 ALU"
$ELSE
            EXECUTE "CREATE.FILE ":BP:" 19"
$ENDIF
            OPEN BP TO BPIO ELSE
               STOP BP:" file not created"
            END
         END
      END
      *
      OPEN "OBJ.UPL" TO OBJ ELSE 
$IFDEF isRT
         EXE =  "CREATE-FILE OBJ.UPL 1,1 7,1 ALU"
$ELSE
         EXE =  "CREATE.FILE OBJ.UPL 19"
$ENDIF
         EXECUTE EXE
         OPEN "OBJ.UPL" TO OBJ ELSE
            STOP "OBJ.UPL file not created"
         END
      END
      *
      OPEN "UPL.INSERTS" TO INSERTS ELSE 
$IFDEF isRT
         EXE =  "CREATE-FILE UPL.INSERTS 1,1 7,1 ALU"
$ELSE
         EXE =  "CREATE.FILE UPL.INSERTS 19"
$ENDIF
         EXECUTE EXE
         OPEN "UPL.INSERTS" TO INSERTS ELSE 
            STOP "UPL.INSERTS file not created"
         END
      END
      *
      IF DO.ALL THEN SELECT BPIO
      *
      LOOP
         READNEXT ID ELSE EXIT
         IF ID = PROG THEN CONTINUE
         IF ID[1,1] = "$" THEN CONTINUE
         ITEM<-1> = ID
      REPEAT
      *
      TODO = DCOUNT(ITEM, @FM)
      PRINT
      PRINT "Compiling ":TODO:" program(s)"
      PRINT
      ABORT = 0
      FAILS = ""
      PASSES= ""
      FOR I = 1 TO TODO
         IF ITEM<I> = "uDO" THEN CONTINUE
         GOSUB DO..INCLUDES
         IF ABORT THEN STOP
         EXE = "BASIC OBJ.UPL ":ITEM<I>
         IF NOT(atHOME) THEN
$IFDEF isRT
            EXE :=  " (S"
$ELSE
            EXE :=  " -I"
$ENDIF
         END
         EXECUTE EXE CAPTURING JUNK
         JUNK = EREPLACE(JUNK, @FM, " ")
         IDX  = INDEX(JUNK, "Compilation Complete", 1)
         IF IDX < 1 THEN
            IF TODO => 2 THEN
               FAILS<-1> = ITEM<I>:"  >> failed"
            END ELSE
               EOJ = DCOUNT(JUNK, @FM)
               FOR JJ = 1 TO EOJ
                  PRINT JUNK<JJ>
               NEXT JJ
            END
            FAILED = 1
         END ELSE 
            IF TODO => 2 THEN
               PASSES<-1> = ITEM<I>:"  >> passed"
            END ELSE
               PRINT ITEM<I>:"  >> passed"
            END
            FAILED = 0
         END
         EXE = "CATALOG OBJ.UPL ":ITEM<I>
$IFNDEF isRT
         EXE :=  " LOCAL"
$ENDIF
         EXECUTE EXE CAPTURING JUNK
         IF (NOT(KEEP)) THEN
            EXE = "DELETE OBJ.UPL ":ITEM<I>
            EXECUTE EXE CAPTURING JUNK
            EXE = "DELETE BP.UPL ":ITEM<I>
            EXECUTE EXE CAPTURING JUNK
         END
      NEXT I
      PRINT
      IF FAILS # "" THEN
         EOI = DCOUNT(FAILS, @FM)
         FOR I = 1 TO EOI
            CRT FAILS<I>
         NEXT I
      END
      PRINT
      PRINT "done"
      EXECUTE "DELETE BP.UPL uDO" CAPTURING JUNK
      EXECUTE "DELETE BP.UPL SLBP" CAPTURING JUNK
      STOP
      *
DO..INCLUDES:
      READ BASE FROM BPIO, ITEM<I> THEN
RESTART..LOOP:
         EOP = DCOUNT(BASE, @FM)
         FOR P = 1 TO EOP
            LINE = TRIMF(BASE<P>)
            *
            IF INDEX(LINE, "COMMON", 1) AND INDEX(LINE, "@@", 1) THEN
               LINE = EREPLACE(LINE, "@@", INSTANCE.ID)
               BASE<P> = LINE
            END
            isINS = 0; isDEF = 0
            isINS = (isINS OR FIELD(LINE, " ", 1) = "$INCLUDE")
            isINS = (isINS OR FIELD(LINE, " ", 1) = "$INSERT")
            isDEF = (FIELD(LINE, " ", 1) = "#DEFINE")
            IF isINS THEN
               READ INSREC FROM INSERTS, FIELD(LINE, " ", 2) ELSE
                  READ INSREC FROM BPIO, FIELD(LINE, " ", 2) ELSE
                     CRT "Cannot find ":FIELD(LINE, " ", 2)
                     ABORT = 1
                     EXIT
                  END
               END
               * In I.COMMON, there may be a string like COMMONS /@@/
               * Replace "@@" with INSTANCE.ID so multiple named commons can co-exist
               INSREC = EREPLACE(INSREC, "@@", INSTANCE.ID)
               BASE<P> = INSREC
               GO RESTART..LOOP
            END
            IF isDEF THEN
               LINE = "$DEFINE "
               BEGIN CASE
                  CASE DBT = "UV"; LINE := "isUV 1"
                  CASE DBT = "UD"; LINE := "isUD 1"
                  CASE DBT = "RT"; LINE := "isRT 1"
                  CASE 1
                     CRT "[ABORT] - Unknown DBT !! "
                     STOP
               END CASE
               BASE<P> = LINE
            END
         NEXT P
         IF NOT(ABORT) THEN WRITE BASE ON OBJ, ITEM<I>
      END
      RETURN
      *
   END
