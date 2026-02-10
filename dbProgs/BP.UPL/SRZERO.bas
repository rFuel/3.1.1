      SUBROUTINE SRZERO (INPUT, OUTPUT)
      true  = 1
      false = 0
      OUTPUT = ""
      FILE = "BP.UPL"
      ITEM = "**"
      GOSUB PREPARE..LOADER
      IF ZERROR THEN STOP
      BP.UPL = IOFILE
      IOFILE = ""
      READ CONTROL FROM BP.UPL, "urLOADER" THEN LOADING=1 ELSE LOADING=0; CONTROL=""
      ZERROR  = false
      * ---------------------------------------------------
      WORDS = CONVERT(INPUT, " ", @VM)
      EVENT = WORDS<1,1>
      BEGIN CASE
         CASE EVENT = "[FI]"
            DELETE BP.UPL, "urLOADER"
            OUTPUT = "200-OK loading is now OFF"
         CASE LOADING
            FILE = FIELD(CONTROL, " ", 1)
            ITEM = FIELD(CONTROL, " ", 2)
            GOSUB LOAD..DATA
            IF ZERROR THEN RETURN
            WRITE FILE:" ":ITEM ON BP.UPL, "urLOADER"
            OUTPUT = "200-OK loading is now ON"
         CASE EVENT = "LOAD"
            FILE = WORDS<1,2>
            ITEM = WORDS<1,3>
            GOSUB PREPARE..LOADER
            IF ZERROR THEN RETURN
            WRITE "" ON IOFILE, ITEM
            WRITE FILE:" ":ITEM ON BP.UPL, "urLOADER"
            OUTPUT = "200-OK load commenced"
         CASE 1
            GOSUB EXECUTE..CMD
            OUTPUT = "200-OK execute complete: ":OUTPUT
      END CASE
      * ---------------------------------------------------
      RETURN
      *
      ****************** [ EXECUTE..CMD ] *****************
      *
EXECUTE..CMD:
      EXECUTE INPUT CAPTURING OUTPUT
      OUTPUT = CONVERT(OUTPUT, @FM, " ")
      OUTPUT = CONVERT(OUTPUT, @VM, " ")
      OUTPUT = CONVERT(OUTPUT, @SM, " ")
      RETURN
      *
      ******************* [ LOAD..DATA ] ******************
      *
LOAD..DATA:
      GOSUB PREPARE..LOADER
      IF ZERROR THEN RETURN
      READ IN.REC FROM IOFILE, ITEM ELSE IN.REC = ""
      IN.REC<-1> = INPUT
      WRITE IN.REC ON IOFILE, ITEM
      RETURN
      *
      **************** [ PREPARE..LOADER ] ****************
      *
PREPARE..LOADER:
      ZERROR = true
      IF FILE = "" THEN
         OUTPUT = "900-Cannot load to null file!"
         RETURN
      END
      IF ITEM = "" THEN
         OUTPUT = "900-Cannot load to file with null ID!"
         RETURN
      END
      ZERROR = false
      TRY  = 1
      LOOP
         IF TRY > 2 THEN 
            ZERROR = true
            OUTPUT = "900-Cannot open/create ":FILE
            RETURN
         END
         OPEN FILE TO IOFILE THEN EXIT
         EXE = "CREATE-FILE ":FILE:" 1,1 7,1 ALU"
         EXECUTE EXE CAPTURING JUNK
         TRY += 1
      REPEAT
      RETURN
   END
