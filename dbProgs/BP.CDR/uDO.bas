      PROMPT ""
$DEFINE isUV 1
      OPEN "BP.UPL" TO BPDATA ELSE
         PRINT "You must CREATE-FILE BP.UPL {options} - see your database guide"
         STOP
      END
      READ DBT FROM BPDATA, "DBT" ELSE DBT = "UV"
      DBT = DBT<1,1,1>
      **********************************************************
      * Usage:                                                 *
      * uDO                   (compiles BP.UPL)                *
      * uDO MY.PROG           (compiles MY.PROG from BP.UPL)   *
      * uDO -F BP.HBS         (compiles BP.HBS)                *
      * uDO -F BP.HBS MY.PROG (compiles MY.PROG from BP.HBS)   *
      **********************************************************
      PROG = "uDO"
      CMD = TRIM(SENTENCE())
      CONVERT " " TO @FM IN CMD
      LOCATE(PROG, CMD; POS) ELSE STOP "Not a valid DO command"
      
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
      IF BP="" THEN 
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
            STOP "OBJ.INSERTS file not created"
         END
      END
      *
      IF DO.ALL THEN SELECT BPIO
      
      LOOP
         READNEXT ID ELSE EXIT
         IF ID = PROG THEN CONTINUE
         IF ID[1,1] = "$" THEN CONTINUE
         ITEM<-1> = ID
      REPEAT
      
      TODO = DCOUNT(ITEM, @FM)
      PRINT
      PRINT "Compiling ":TODO:" program(s)"
      PRINT
      ABORT = 0
      FAILS = ""
      PASSES= ""
      FOR I = 1 TO TODO
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
         IF INDEX(JUNK, "Compilation completed", 1) < 1 THEN 
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
      
DO..INCLUDES:
      READ BASE FROM BPIO, ITEM<I> THEN
RESTART..LOOP:
         EOP = DCOUNT(BASE, @FM)
         FOR P = 1 TO EOP
            LINE = TRIMF(BASE<P>)
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
   END
