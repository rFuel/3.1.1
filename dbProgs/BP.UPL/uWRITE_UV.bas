      SUBROUTINE uWRITE_UV(ANS, uDBT, LIST, LISTIO)
$INCLUDE I_Prologue
      IF DBT # "UV" THEN
         ANS = "UV writer called for DB Type ":DBT:" - FATAL"
      END ELSE
         ANS = ""
         WRITEBLK LIST ON LISTIO ELSE
            ANS = " WRITEBLK failed"
         END
      END
      RETURN
   END
