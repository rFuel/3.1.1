      SUBROUTINE SR.SCRUBNARRATIVE ( OLDSTRING, NEWSTRING )
      *
      * Modify narrative to sanitise against PII.
      *
      IF OLDSTRING="" THEN NEWSTRING=OLDSTRING; RETURN
      *
      MARKERS = "TFR to ":@FM
      MARKERS:= "TFR TO ":@FM
      MARKERS:= "TFR from ":@FM
      MARKERS:= "MOB to-":@FM
      MARKERS:= "Web PP To-":@FM
      MARKERS:= "Web tf PP To-":@FM
      MARKERS:= "Web PP Ref-":@FM
      MARKERS:= "Web To-":@FM
      MARKERS:= "NPP to ":@FM
      MARKERS:= "NPP From ":@FM
      MARKERS:= "MOB ":@FM
      MARKERS:= "Web ":@FM
      MARKERS:= "Ref-":@FM
      MARKERS:= "BPAY: ":@FM
      MARKERS:= "From: ":@FM
      MARKERS:= "Ref: ":@FM
      MARKERS:= "Declined by "
      EOMK = DCOUNT(MARKERS, @FM)
      * ------------------------------------------------------------
      *
      LX = LEN(OLDSTRING)+1
      I  = 0
      NEWSTRING = ""
      MASK      = "*"
      SPLIT     = @FM:"~"
      FOR M = 1 TO EOMK
         STG = MARKERS<M>
         MKR = "~~H":M:"~."
         OLDSTRING = EREPLACE(OLDSTRING, STG, MKR)
      NEXT M
      * ------------------------------------------------------------
      *
      ARRAY = EREPLACE(OLDSTRING, "~~", SPLIT)
      EOX = DCOUNT(ARRAY, @FM)
      NEWSTRING = ""
      FOR X = 1 TO EOX
         LINE = ARRAY<X>
         MKR  = ""
         IF LINE[1,2] = "~H" THEN
            * get the ~Hn~. and grab the "n"
            MKR = FIELD(LINE, ".", 1)
            LINE= LINE[LEN(MKR)+2, LEN(LINE)]
            MKR = EREPLACE(MKR, ".", "")
            MKR = EREPLACE(MKR, "~", "")
            MKR = EREPLACE(MKR, "H", "")
            MKR = MARKERS<MKR>
            * strip the ~Hn~.
***         LINE = LINE[INDEX(LINE, ".", 1), LEN(LINE)]
         END
         NEW="" ; CALL SR.SCRAMBLESTRING( LINE, NEW )
         LINE = NEW
         LINE = MKR:LINE
         NEWSTRING := LINE
      NEXT X
      RETURN
   END
