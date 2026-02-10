      SUBROUTINE SR.GET.PROPERTY (prop, ans)
$INSERT I.SOCKET.COMMON
      *
$IFDEF isRT
      MATBUILD sPROPS FROM sockPROPS
      eoi = DCOUNT(sPROPS, @FM)
$ELSE
      eoi = INMAT(sockPROPS)<1,1,1>
$ENDIF
      ans = ''
      FOR i = eoi TO 1 STEP -1
$IFDEF isRT
         cFIELD = sPROPS<i>
$ELSE
         cFIELD = sockPROPS(i)
$ENDIF
         IF cFIELD # "" THEN
            key = FIELD(cFIELD, "=", 1)
            IF UPCASE(key) = UPCASE("msg_":prop) THEN
               ans = FIELD(cFIELD, "=", 2)
               EXIT
            END ELSE
               IF UPCASE(key) = UPCASE(prop) THEN
                  ans = FIELD(cFIELD, "=", 2)
                  EXIT
               END
            END
         END
      NEXT i
      RETURN
   END
