      *
      EXECUTE "WHO" CAPTURING OUTPUT
      THISACT = FIELD(OUTPUT, " ", 2)
      CMD = @SENTENCE
      CMD = EREPLACE(CMD, " ", @FM)
      LOCATE("SWITCH.TO", CMD; FND) ELSE STOP "Error"
      PTR = CMD<FND+1>
      OPEN "VOC" TO VOC ELSE STOP "No VOC file"
      OPEN "BP.UPL" TO BP.UPL ELSE STOP "BP.UPL"
      *
      READ FLIST FROM BP.UPL, "SANITISE-FILES" ELSE FLIST = ""
      *
      POINTER = "Q":@FM:THISACT
      FLIST = EREPLACE(FLIST, "~", @FM)
      F = 0
      LOOP
         F += 1
         ID = TRIM(FLIST<F>)
      UNTIL ID = "" DO
         DNAME = PTR:".":ID
         POINTER<3> = DNAME
         WRITE POINTER ON VOC, ID
      REPEAT
      STOP
   END