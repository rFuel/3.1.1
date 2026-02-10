      SUBROUTINE SR.GET.HOSTNAME(FAIL, HOSTNAME)
      FAIL = 0
      IF HOSTNAME # "" THEN RETURN
      *
      EXE = "SH -c'hostname'"
      EXECUTE EXE CAPTURING HOSTNAME
      IF HOSTNAME = "" THEN
         HOSTNAME = "Cannot establish hostname"
         FAIL = 1
      END
      RETURN
   END