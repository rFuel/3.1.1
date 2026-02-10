      * UniVerse and UniData only
      * ------------------------------------------------------------- *
      * Usage: MONITOR {file} {ON/OFF}                                 
      * ------------------------------------------------------------- *
      ERR = ""
      OPEN "VOC" TO VOC ELSE
         ERR = "Cannot open VOC"
         GO END..PROG
      END
      CALL SR.OPEN.CREATE (ERR, "TRIGGERS", "DYNAMIC", TFILE)
      IF ERR THEN
         ERR = "Cannot open TRIGGERS file"
         GO END..PROG
      END
      *
      SENTENCE = TRIM(@SENTENCE)
      CONVERT " " TO @FM IN SENTENCE
      EOI = DCOUNT(SENTENCE, @FM)
      FOR I = 1 TO EOI
         JUNK = SENTENCE<1>[2,99]
         IF SENTENCE<1>[2,99] = "MONITOR" THEN 
            SENTENCE = DELETE(SENTENCE, 1)
            EXIT
         END ELSE
            SENTENCE = DELETE(SENTENCE, 1)
         END
      NEXT I
      FILE = SENTENCE<1>
      FLAG = SENTENCE<2>
      *
      IF FLAG # "ON" AND FLAG # "OFF" THEN
         CRT "ERROR: use 'ON' or 'OFF' to set monitoring on/off"
         CRT "       your setting was ":FLAG
         CRT "       no action taken."
         CRT
         GO END..PROG
      END
      OPEN FILE TO CHECK.IO ELSE
         ERR = "Cannot open ":FILE
         GO END..PROG
      END
      *
      CRT "Actions taken:"
      CRT
      CRT "  1. ":FILE:" has passed verification."
      CRT "  2. Trigger place-holders have been ":
      *
      OUTPUT = ""
      IF FLAG = "ON" THEN
         CRT "created ":
         ibEXE = 'CREATE TRIGGER iatUPL BEFORE INSERT ON ':FILE:' FOR EACH ROW CALLING "*uTRIGGER";'
         ubEXE = 'CREATE TRIGGER uatUPL BEFORE UPDATE ON ':FILE:' FOR EACH ROW CALLING "*uTRIGGER";'
         dbEXE = 'CREATE TRIGGER datUPL BEFORE DELETE ON ':FILE:' FOR EACH ROW CALLING "*uTRIGGER";'
         * -----------------------------------------------------------------------------------------
         iaEXE = 'CREATE TRIGGER iatUPL AFTER INSERT ON ':FILE:' FOR EACH ROW CALLING "*uTRIGGER";'
         uaEXE = 'CREATE TRIGGER uatUPL AFTER UPDATE ON ':FILE:' FOR EACH ROW CALLING "*uTRIGGER";'
         daEXE = 'CREATE TRIGGER datUPL AFTER DELETE ON ':FILE:' FOR EACH ROW CALLING "*uTRIGGER";'
         EXECUTE ibEXE CAPTURING OUT1;  OUT1 = "BEFORE Insert: ":OUT1
         EXECUTE ubEXE CAPTURING OUT2;  OUT2 = "BEFORE Update: ":OUT2
         EXECUTE dbEXE CAPTURING OUT3;  OUT3 = "BEFORE Delete: ":OUT3
         EXECUTE iaEXE CAPTURING OUT4;  OUT4 = "AFTER  Insert: ":OUT4
         EXECUTE uaEXE CAPTURING OUT5;  OUT5 = "AFTER  Update: ":OUT5
         EXECUTE daEXE CAPTURING OUT6;  OUT6 = "AFTER  Delete: ":OUT6
         OUTPUT = OUT1:@FM:OUT2:@FM:OUT3:@FM:OUT4:@FM:OUT5:@FM:OUT6
      END
      *
      IF FLAG = "OFF" THEN
         CRT "deleted ":
         EXE = "DROP TRIGGER ":FILE:" ALL;"
         EXECUTE EXE CAPTURING OUTPUT
         * Re-instate any pre-existing triggers using the uTRIGGERS file.
      END
      CRT
      CRT "with these results: "
      EOO = DCOUNT(OUTPUT, @FM)
      FOR O = 1 TO EOO
         IF TRIM(OUTPUT<O>) # "" THEN
            CRT "::> ":OUTPUT<O>
         END
      NEXT O
      *
      IF FLAG = "ON" THEN
         CRT
         CRT "Now maintain these items in the TRIGGERS file:-"
         CRT "   1. I_":FILE:"   to catch Insert events"
         CRT "   2. U_":FILE:"   to catch Update events"
         CRT "   3. D_":FILE:"   to catch Delete events"
         CRT "   ---------------------------------------------"
         CRT "NB: all subroutines to be called by uTRIGGER"
         CRT "    MUST be cataloged !!!"
         CRT "   ---------------------------------------------"
      END
      CRT
      CLOSE VOC
END..PROG:
      IF ERR # "" THEN CRT ; CRT ERR
      CRT
      STOP
   END
