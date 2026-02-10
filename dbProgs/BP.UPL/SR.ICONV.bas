      SUBROUTINE SR.ICONV(ERR, CV, NEW.REC, OLD.REC, IN.REC)
      * ----------------------------------------------------------------
      * Take IN.REC, apply CV conversions to it and the OLD.REC         
      *      thus creating NEW.REC - the return value                   
      *      which will be put back into REC.ARR(nn)<AV,VM,SV>          
      * ----------------------------------------------------------------
      ERR = ""
      NEW.REC = OLD.REC
      DATUM = IN.REC
      CV.PARTS = EREPLACE(CV, "}", @VM)
      NBR.CV = DCOUNT(CV.PARTS, @VM)
      FOR idx = 1 TO NBR.CV
         CV = CV.PARTS<1,idx>
         IF UPCASE(CV[1,4]) = "MATH" THEN
            IF NUM(OLD.REC) THEN
               OPER = FIELD(FIELD(CV, "(", 2), ")", 1)
               TMP = ""
               BEGIN CASE
                  CASE OPER = "+"
                     TMP = OLD.REC + DATUM
                  CASE OPER = "-"
                     TMP = OLD.REC - DATUM
                  CASE OPER = "*"
                     TMP = OLD.REC * DATUM
                  CASE OPER = "/"
                     TMP = OLD.REC / DATUM
                  CASE 1
                     ERR = " Invalid MATH operation in [":CV:"]"
                     RETURN
               END CASE
               TMP = ICONV(TMP, "MD0")
            END ELSE
               ERR = " MATH operation on non-numeric data field - ABORT. [":CV:"]"
               RETURN
            END
         END ELSE
            TMP = ICONV(DATUM, CV)
            IF TMP = "" THEN TMP = DATUM
         END
         DATUM = TMP
      NEXT idx
      NEW.REC = DATUM
      RETURN
   END
