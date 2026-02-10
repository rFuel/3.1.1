      PROMPT ""
      CRT @(-1)
      REMOTE = "RFUELPP"
      LOCAL = "MASKPP"
      PIF = "S4"                         ; * Product In Focus
      *
      CMD = @SENTENCE
      CMD = EREPLACE(CMD, " ", @FM)
      LOCATE("SCRUB.FILE", CMD; FND) ELSE STOP
      FND += 1
      FILE = CMD<FND>
      IF FILE = "" THEN CRT "Which File? " ; STOP
      *
      PIFSEL = "SELECT REAL.$ LIKE ...":PIF:" OR LIKE ...":PIF:"/... OR LIKE ...":PIF:"...."
      PIFSEL = EREPLACE(PIFSEL, "$", FILE)
      *
      SCRUB = "SANITISE-V2 FROM ":REMOTE:" FILE $ LIST $ CSV $.csv"
      SCRUB = EREPLACE(SCRUB, "$", FILE)
      *
      LPOINT = "SET.FILE ":LOCAL:" ":FILE:" ":FILE
      RPOINT = "SET.FILE ":REMOTE:" ":FILE:" REAL.":FILE
      SAVLIST = "SAVE.LIST ":FILE
      *
      DELQPTR = "DELETE VOC REAL.":FILE
      DELLPTR = "DELETE VOC ":FILE
      DELLIST = "DELETE.LIST ":FILE
      *
      CRT RPOINT
      EXECUTE RPOINT
      CRT LPOINT
      EXECUTE LPOINT
      CRT PIFSEL
      EXECUTE PIFSEL
      CRT SAVLIST
      EXECUTE SAVLIST
      CRT SCRUB
      EXECUTE SCRUB
      CRT " "
      CRT DELLIST
      EXECUTE DELLIST
      CRT DELQPTR
      EXECUTE DELQPTR
      CRT DELLPTR
      EXECUTE DELLPTR
      STOP
   END
