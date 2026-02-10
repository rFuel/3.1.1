      PROMPT ""
      OPEN "DATA.EXPORT" TO DATA.IN ELSE STOP "DATA.EXPORT missing"
$DEFINE isRT 1
$IFDEF isRT
      VFILE = "MD"
      CMD = SENTENCE()
$ELSE
      VFILE = "VOC"
      CMD = @SENTENCE
$ENDIF
      OPEN VFILE TO VOC ELSE STOP "[":VFILE:"] is missing"
      *
10    *
      CRT
      CRT "In which Account will these files reside : ":
      INPUT ACCT
      IF ACCT="" OR ACCT="Q" OR ACCT = "END" THEN CRT; STOP
      QPTR = "Q":@FM:ACCT:@FM:VFILE
      WRITE QPTR ON VOC, "QFL"
      OPEN "QFL" TO REMOTE.VOC ELSE
         CRT
         CRT ACCT:" is not a valid Account. Try again."
         GO 10
      END
      DELETE VOC, "QFL"
      CRT
      *
20    *
      CRT "Okay to import data now (Y/n) ":
      INPUT OK
      IF OK # "Y" THEN STOP
      *
      SELECT DATA.IN
      FILES = ""
      LOOP
         READNEXT ITEM ELSE EXIT
         FILE = FIELD(ITEM, "~", 1)
         LOCATE(FILE, FILES; FND) ELSE 
            FILES<-1> = FILE
            CRT "Added ":FILE:" to the import list"
         END
      REPEAT
      IF FILES = "" THEN STOP "Nothing to do."
      EOF = DCOUNT(FILES, @FM)
      *
$IFDEF isRT
      DIM FHANDLES(500)
$ELSE
      DIM FHANDLES(EOF)
$ENDIF
      *
      MAT FHANDLES = ""
      *  
      * ------------------------------
      *  
      F = 0
      ERR = 0
      LOOP
         F += 1
         IF FILES<F> = "" THEN EXIT
         FILE = FILES<F>
         QPTR = "Q":@FM:ACCT:@FM:FILE
         QID = "q_":FILE
         WRITE QPTR ON VOC, QID
         OPEN QID TO FHANDLES(F) ELSE
            CRT ">> ":FILE:" is not a valid file-name. Create the file, then try again."
            ERR = 1
         END
      REPEAT
      IF ERR THEN 
         CRT "Fix the errors then try again."
$IFDEF isRT
         NULL
$ELSE
         CLOSE
$ENDIF
         STOP
      END
      TOTALS = ""
      SELECT DATA.IN
      LOOP
         READNEXT ITEM ELSE EXIT
         FILE = FIELD(ITEM, "~", 1)
         ID   = FIELD(ITEM, "~", 2)
         LOCATE(FILE, FILES; FND) THEN 
            IF FHANDLES(FND) = "" THEN
               CRT FILE:" has not been opened ... skipping it."
               CONTINUE
            END
            TOTALS<FND> += 1
         END ELSE
            CRT "cannot find ":FILE:" ... skipping it."
            CONTINUE
         END
         READ REC FROM DATA.IN, ITEM THEN
            REC = EREPLACE(REC, "<fm>", @FM)
            REC = EREPLACE(REC, "<vm>", @VM)
            REC = EREPLACE(REC, "<sm>", @SM)
            WRITE REC ON FHANDLES(FND), ID
         END ELSE
            CRT ITEM:" has gone missing from DATA.EXPORT"
            CONTINUE
         END
      REPEAT
      CRT "Done."
      FOR I = 1 TO EOF
         CRT TOTALS<I> "R#10":" loaded into ":FILES<I>
      NEXT I
      STOP
   END
