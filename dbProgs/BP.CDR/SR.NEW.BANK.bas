      SUBROUTINE SR.NEW.BANK ( ERR, NAME, PASSWORD )
      *
      ERR = ""
      IF NAME = "" THEN ERR = "No Account name!"; GO END..SRTN
      IF PASSWORD = "" THEN ERR = "No Password given!"; GO END..SRTN
      *
      OPEN "SYSTEM" TO SYSTEM.FILE ELSE ERR = "Cannot find SYSTEM file."; GO END..SRTN
      READ CHECK FROM SYSTEM.FILE, NAME THEN
         ERR = "[":NAME:"] exists - cannot create this database account."
         GO END..SRTN
      END
      *
      ACNAME = NAME
      RETCDS = ""
      UPDCDS = ""
      PASSWD = PASSWORD
      SYSPRV = ""
      FILUPD = ""
      MODSEP = ""
      DBGLVL = ""
      EXE = "CREATE-ACCOUNT":@FM:"L":@FM:ACNAME:@FM:RETCDS:@FM:UPDCDS:@FM:PASSWD:@FM:PASSWD
      EXE:= @FM:SYSPRV:@FM:FILUPD:@FM:MODSEP:@FM:@FM:@FM:@FM:@FM
      *
      EXECUTE EXE CAPTURING JUNK
      ERR = JUNK
      READ CHECK FROM SYSTEM.FILE, NAME ELSE
         ERR = "[":NAME:"] - FAILED to create this database account."
         GO END..SRTN
      END
END..SRTN:
      ERR = EREPLACE(ERR, @FM, "")
      RETURN
   END

