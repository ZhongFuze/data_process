#!/bin/sh
curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/0x99C19AB10b9EC8aC6fcda9586E81f6B73a298870
# Return Value: {"domainName":"test123.gno"}Time: 4.470421s
