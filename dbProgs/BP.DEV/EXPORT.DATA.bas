      PROMPT ""
      OPEN "BP.UPL" TO BP.UPL ELSE STOP "BP.UPL missing"
      READV DBT FROM BP.UPL, "DBT",1  ELSE DBT = ""
      DBT = UPCASE(DBT)
      VFILE  = "VOC"
      IF DBT = "RT" THEN VFILE = "MD"
      *
      OPEN "DATA.EXPORT" TO DATA.OUT ELSE STOP "DATA.EXPORT missing"
      OPEN VFILE TO VOC ELSE STOP VFILE:"cannot be opened"
      FILES    = ""
      *
10    *
      CRT
      CRT "        ... for the Q-pointer,"
      CRT "        In which Account do the files reside : ":
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
      CRT "File-name to add to the list of export files : ":
      INPUT FILE
      IF FILE="" OR FILE="Q" OR FILE = "END" THEN GO 30
      QPTR = "Q":@FM:ACCT:@FM:FILE
      WRITE QPTR ON VOC, "QFL"
      OPEN "QFL" TO REMOTE.FILE ELSE
         CRT
         CRT ">> ":FILE:" is not a valid file-name. Try again."
         GO 20
      END
      DELETE VOC, "QFL"
      FILES<-1> = FILE
      GO 20
      *
30    *
      IF FILES = "" THEN CRT; CRT ; GO 10
      *  
      * ------------------------------
      *  
      F = 0
      LOOP
         F += 1
         FILE = FILES<F>
         IF FILE = "" THEN EXIT
         QPTR = "Q":@FM:ACCT:@FM:FILE
         WRITE QPTR ON VOC, "QFL"
         OPEN "QFL" TO FILEIO ELSE
            CRT "cannot open ":FILE
            CONTINUE
         END
         CRT "Loading ... ":FILE
         SELECT FILEIO
***      EXE = "SSELECT QFL"
***      EXECUTE EXE CAPTURING JUNK
         LOOP
            READNEXT ID ELSE EXIT
            READ REC FROM FILEIO, ID ELSE CONTINUE
            REC = EREPLACE(REC, @FM , "<fm>")
            REC = EREPLACE(REC, @VM , "<vm>")
            REC = EREPLACE(REC, @SM , "<sm>")
            KEY = FILE:"~":ID
            WRITE REC ON DATA.OUT, KEY
         REPEAT
      REPEAT
      STOP
   END
