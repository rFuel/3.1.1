      PROMPT ""
      * ---------------------------------------------------------------------------
      * Useage: FILE.HANDLER FILE=filename BLOCK=block-size
      * ---------------------------------------------------------------------------
      PROG = "FILE.HANDLER"
      OPEN "VOC" TO VOC ELSE STOP "VOC open error."
      OPEN "#LOADER#" TO LOADER ELSE STOP "#LOADER# open error."
      CMD = EREPLACE(TRIM(@SENTENCE), " ", @FM)
      LOCATE(PROG, CMD; POS) THEN
         FOR I = 1 TO POS
            CMD = DELETE(CMD, 1)
         NEXT I
      END
      FILE = ""
      BLOCK= 0
      EOI = DCOUNT(CMD, @FM)
      FOR I = 1 TO EOI
         IF UPCASE(FIELD(CMD<I>, "=", 1)) = "FILE"  THEN FILE  = FIELD(CMD<I>, "=", 2)
         IF UPCASE(FIELD(CMD<I>, "=", 1)) = "BLOCK" THEN BLOCK = FIELD(CMD<I>, "=", 2)
      NEXT I
      IF TRIM(FILE) = "" THEN STOP "No file has been given."
      IF BLOCK = 0 THEN BLOCK = 100
      OPEN FILE TO IOFILE ELSE STOP FILE:" open error."
      SMARK  = "<sm>"
      VMARK  = "<vm>"
      FMARK  = "<fm>"
      IMARK  = "<im>"
      OUTREC = ""
      CTR    = 0
      CNT1   = 0
      CNT2   = 0
      SELECT IOFILE
      LOOP
         READNEXT ID ELSE EXIT
         READ REC FROM IOFILE, ID ELSE CONTINUE
         CTR += 1
         REC = EREPLACE(REC, @SM, SMARK)
         REC = EREPLACE(REC, @VM, VMARK)
         REC = EREPLACE(REC, @FM, FMARK)
         REC = ID:IMARK:REC
         CNT1 += 1
         OUTREC := REC:@FM
         IF CNT1 = BLOCK THEN
            CNT2 += 1
            KEY = FILE:"-":CNT2
            WRITE OUTREC ON LOADER, KEY
            CRT CNT1:" records going into ":KEY
            OUTREC = ""
            CNT1 = 0
         END
      REPEAT
      IF OUTREC # "" THEN
         CNT2 += 1
         KEY = FILE:"-":CNT2
         WRITE OUTREC ON LOADER, KEY
         CRT CNT1:" records going into ":KEY
      END
      CRT CTR:" records loaded into #LOADER#"
      STOP
      * ---------------------------------------------------------------------------
   END