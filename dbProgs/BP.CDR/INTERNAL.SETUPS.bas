      * -------------------------------------------------------
      * NB: This program is loaded, compiled and run by SetupDB
      *     Purpose: Create internal assests which cannot be 
      *              created during SetupDB (jar) run.
      * -------------------------------------------------------
      *
      OPEN "DICT", "CDR.WORKFILE" TO DICTIO ELSE STOP "FAIL. Cannot open DICT CDR.WORKFILE"
      *
      REC = ""
      REC<1> = "A,0,Group1,,,,,G0*1,L,10"
      REC<2> = "A,0,Group2,,,,,G1*1,L,10"
      REC<3> = "A,0,Group3,,,,,G2*1,L,10"
      CRT ""
      CRT "------------------------------------------"
      CRT "1.  Create DICT CDR.WORKFILE items:"
      CRT "------------------------------------------"
      *
      KEY = "GRP"
      FOR I = 1 TO 3
         DREC = REC<I>
         DREC = EREPLACE(DREC, ",", @FM)
         ID = KEY:I
         WRITE DREC ON DICTIO, ID
         CRT "      ":ID:"  ":DREC<3>:"  done"
      NEXT I
      CRT ""
      *
      * -------------------------------------------------------
      *
      OPEN "BP.UPL" TO BPIO ELSE STOP "FAIL. Cannot open BP.UPL"
      * get rid of the trailing linefeed
      WRITE "RT" ON BPIO, "DBT"
      READ CHK FROM BPIO, "DBT" ELSE CHK="ERROR"
      CRT ""
      CRT "------------------------------------------"
      CRT "2.  BP.UPL DBT [":CHK:"]"
      CRT "------------------------------------------"
      CRT ""
      *
      READ CHK FROM BPIO, "CDR-ACCOUNT-TYPES" ELSE CHK = ""
      CRT ""
      CRT "------------------------------------------"
      CRT "3.  BP.UPL CDR-ACCOUNT-TYPES  [":EREPLACE(CHK, @FM, ", "):"]"
      CRT "------------------------------------------"
      IF DCOUNT(CHK, @FM) < 2 THEN
         CRT "   WARN: CDR-ACCOUNT-TYPES may not be setup.."
      END
      CRT ""
      *
      CRT "finished."
      STOP
   END

